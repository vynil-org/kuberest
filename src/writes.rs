//! Per-item processing of the `writes` groups.
//!
//! The reconcile loop renders the templates and hands each item to [`process_item`], which
//! decides what to do ([`decide`], a pure function), performs the HTTP calls through the
//! [`ObjApi`] trait (so tests can use an in-memory fake) and reports the outcome as an
//! [`ItemResult`]: the object to expose as `write.<group>.<item>`, the key to remember, the
//! conditions raised and the pending asynchronous task, if any.
use crate::restendpoint::{ApplicationCondition, OwnedRestPoint};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use serde_json_path::JsonPath;
use vynil_core::{
    Error as CoreError,
    http::{CreateMethod, ReadMethod, RestClient, UpdateMethod},
};

/// How to compare the observed object with the desired values before updating it
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema, Default)]
pub enum DiffMode {
    /// No comparison: the object is updated at every reconcile (default)
    #[default]
    None,
    /// Every key of the desired values must match, extra keys on the remote side are ignored
    Subset,
    /// Like Subset, but extra keys on the remote side are a difference too
    Exact,
}

/// Asynchronous writes: the API answers with a reference to a task instead of the object
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AsyncWrite {
    /// JSON path of the task reference in the response of a write (e.g. `$.task`)
    pub task_path: String,
    /// JSON path of the state in the task object (e.g. `$.state`)
    pub state_path: String,
    /// States meaning the task is finished successfully (e.g. `[completed]`)
    pub done: Vec<String>,
    /// States meaning the task has failed (e.g. `[failed, canceled]`)
    pub failed: Vec<String>,
    /// JSON path, in a finished task, of the reference of the created object (e.g. `$.created_resources[0]`).
    /// Without it the object is found back through the item's readPath.
    pub result_path: Option<String>,
}

/// The HTTP operations needed to maintain an object
pub trait ObjApi {
    /// Read an object (GET)
    fn read(&mut self, method: ReadMethod, path: &str, key: &str) -> vynil_core::Result<Value>;
    /// Create an object
    fn create(&mut self, method: CreateMethod, path: &str, input: &Value) -> vynil_core::Result<Value>;
    /// Update an object
    fn update(
        &mut self,
        method: UpdateMethod,
        path: &str,
        key: &str,
        input: &Value,
    ) -> vynil_core::Result<Value>;
}
impl ObjApi for RestClient {
    fn read(&mut self, method: ReadMethod, path: &str, key: &str) -> vynil_core::Result<Value> {
        self.obj_read(method, path, key)
    }

    fn create(&mut self, method: CreateMethod, path: &str, input: &Value) -> vynil_core::Result<Value> {
        self.obj_create(method, path, input)
    }

    fn update(
        &mut self,
        method: UpdateMethod,
        path: &str,
        key: &str,
        input: &Value,
    ) -> vynil_core::Result<Value> {
        self.obj_update(method, path, key, input, false)
    }
}

/// Everything [`process_item`] needs to know about an item, already resolved
/// (group value overriding client value, templates rendered)
pub struct ItemCtx {
    pub group: String,
    pub name: String,
    /// Rendered path of the group
    pub path: String,
    pub update_path: Option<String>,
    pub key_name: String,
    pub key_use_slash: bool,
    pub key_is_path: bool,
    pub children_no_sub_key: bool,
    /// Rendered static key of the item: the object lives at `path/<key>`
    pub static_key: Option<String>,
    pub adopt: bool,
    pub diff: DiffMode,
    pub values_query: Option<String>,
    pub read_values_query: Option<String>,
    pub read_path: Option<String>,
    pub read_json_query: Option<String>,
    pub create_method: CreateMethod,
    pub read_method: ReadMethod,
    pub update_method: UpdateMethod,
    pub async_write: Option<AsyncWrite>,
    /// Path part of the client's baseurl (stripped from hrefs when `key_is_path`)
    pub base_path: String,
}

/// Outcome of [`process_item`]
#[derive(Default, Debug)]
pub struct ItemResult {
    /// Value exposed as `write.<group>.<item>`
    pub obj: Value,
    /// Key to remember (empty when the object is not known yet)
    pub key: String,
    /// Reference of the asynchronous task still running
    pub pending_task: Option<String>,
    /// The item waits for a task: the following items must not be processed
    pub pending: bool,
    /// The object has disappeared (404 on update), it is neither kept nor retried
    pub giveup: bool,
    pub conditions: Vec<ApplicationCondition>,
}

/// What to do with an item
#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    /// Nothing is known about the object: create it
    Create,
    /// The object is known under this key: update it
    Update(String),
    /// The object is known and already as desired: do nothing
    Skip(String),
}

/// Decide what to do. `key` is the key of the object when it is known (owned or adopted),
/// `same` tells whether the observed object matches the desired values (None: not observed).
#[must_use]
pub fn decide(key: Option<String>, diff: &DiffMode, same: Option<bool>) -> Decision {
    match key {
        None => Decision::Create,
        Some(k) => {
            if *diff != DiffMode::None && same == Some(true) {
                Decision::Skip(k)
            } else {
                Decision::Update(k)
            }
        }
    }
}

fn num_eq(a: &serde_json::Number, b: &serde_json::Number) -> bool {
    a == b || (a.as_f64().is_some() && a.as_f64() == b.as_f64())
}

/// True when every key of `desired` is found in `observed` with the same value (recursively
/// for objects, strictly for arrays and scalars). A desired `null` matches a missing key.
#[must_use]
pub fn json_subset(observed: &Value, desired: &Value) -> bool {
    match (observed, desired) {
        (Value::Object(o), Value::Object(d)) => d.iter().all(|(k, v)| match o.get(k) {
            Some(ov) => json_subset(ov, v),
            None => v.is_null(),
        }),
        (Value::Number(a), Value::Number(b)) => num_eq(a, b),
        (a, b) => a == b,
    }
}

/// Like [`json_subset`], but the objects must have the same keys too (null equals missing)
#[must_use]
pub fn json_equal(observed: &Value, desired: &Value) -> bool {
    match (observed, desired) {
        (Value::Object(o), Value::Object(d)) => {
            d.iter().all(|(k, v)| match o.get(k) {
                Some(ov) => json_equal(ov, v),
                None => v.is_null(),
            }) && o.iter().all(|(k, v)| d.contains_key(k) || v.is_null())
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| json_equal(x, y))
        }
        (Value::Number(a), Value::Number(b)) => num_eq(a, b),
        (a, b) => a == b,
    }
}

/// First result of a JSON path query
#[must_use]
pub fn query_one(value: &Value, query: &str) -> Option<Value> {
    JsonPath::parse(query)
        .ok()
        .and_then(|p| p.query(value).all().into_iter().next().cloned())
}

/// Apply an optional query, keeping the whole value when it matches nothing or is unusable
#[must_use]
pub fn query_or_whole(value: &Value, query: Option<&str>) -> Value {
    match query {
        Some(q) => JsonPath::parse(q)
            .map(|p| {
                p.query(value)
                    .at_most_one()
                    .unwrap_or(Some(value))
                    .unwrap_or(value)
                    .clone()
            })
            .unwrap_or_else(|_| value.clone()),
        None => value.clone(),
    }
}

/// Path part of a url (no trailing slash), or an empty string
#[must_use]
pub fn base_path_of(baseurl: &str) -> String {
    let rest = baseurl.split_once("://").map_or(baseurl, |(_, r)| r);
    match rest.find('/') {
        Some(i) => rest[i..].trim_end_matches('/').to_string(),
        None => String::new(),
    }
}

/// Turn an href (`/pulp/api/v3/remotes/1/`, or a full url) into a path relative to the client's
/// baseurl: the scheme and host are dropped, then the path of the baseurl, then the leading `/`.
/// The trailing `/` is kept.
#[must_use]
pub fn rel_path(href: &str, base_path: &str) -> String {
    let mut p = href;
    if let Some((_, r)) = p.split_once("://") {
        p = r.find('/').map_or("", |i| &r[i..]);
    }
    if !base_path.is_empty()
        && let Some(rest) = p.strip_prefix(base_path)
        && (rest.is_empty() || rest.starts_with('/'))
    {
        p = rest;
    }
    p.trim_start_matches('/').to_string()
}

/// A key as the API returns it (string or number)
///
/// # Errors
/// When the key is missing, or neither a string nor a number
pub fn extract_key(obj: &Value, key_name: &str) -> Result<String, String> {
    let v = &obj[key_name];
    if let Some(n) = v.as_number() {
        Ok(n.to_string())
    } else if let Some(s) = v.as_str() {
        Ok(s.to_string())
    } else {
        Err(format!("{key_name} is neither a number or a string in {obj:}"))
    }
}

fn is_404(e: &CoreError) -> bool {
    matches!(e, CoreError::MethodFailed(_, 404, _))
}

struct Run<'a, A: ObjApi> {
    api: &'a mut A,
    cx: &'a ItemCtx,
    res: ItemResult,
}

impl<A: ObjApi> Run<'_, A> {
    fn fail(&mut self, what: &str, e: &CoreError) {
        self.res
            .conditions
            .push(ApplicationCondition::write_failed(&format!(
                "{what} write.{}.{} raised {e:?}",
                self.cx.group, self.cx.name
            )));
    }

    /// The key as stored: a relative href, or the key followed by a slash when asked
    fn decorate(&self, key: &str) -> String {
        if self.cx.key_is_path {
            rel_path(key, &self.cx.base_path)
        } else if self.cx.key_use_slash {
            format!("{key}/")
        } else {
            key.to_string()
        }
    }

    /// Where an object can be read: readPath, else its own url when its key is known
    fn read_target(&self, key: Option<&str>) -> Option<(String, String)> {
        if let Some(rp) = &self.cx.read_path {
            return Some((rp.clone(), String::new()));
        }
        let k = key?;
        if self.cx.key_is_path {
            Some((k.to_string(), String::new()))
        } else if self.cx.children_no_sub_key {
            None
        } else {
            Some((
                self.cx.update_path.clone().unwrap_or(self.cx.path.clone()),
                k.to_string(),
            ))
        }
    }

    /// Read an object. Ok(None) when it does not exist (404 or an empty answer).
    fn read(&mut self, target: &(String, String)) -> Result<Option<(Value, Value)>, CoreError> {
        match self
            .api
            .read(self.cx.read_method.clone(), target.0.as_str(), target.1.as_str())
        {
            Ok(raw) => {
                let obj = query_or_whole(&raw, self.cx.read_json_query.as_deref());
                Ok(Some((raw, obj)))
            }
            Err(e) if is_404(&e) => Ok(None),
            Err(CoreError::JsonError(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Re-read an object after a write, raising ReReadFailed on error
    fn reread(&mut self, target: &(String, String)) -> Value {
        match self.read(target) {
            Ok(Some((_, obj))) => obj,
            Ok(None) => json!({}),
            Err(e) => {
                self.res
                    .conditions
                    .push(ApplicationCondition::reread_failed(&format!(
                        "Reading back write.{}.{} raised {e:?}",
                        self.cx.group, self.cx.name
                    )));
                json!({})
            }
        }
    }

    fn task_of(&self, resp: &Value) -> Option<String> {
        let a = self.cx.async_write.as_ref()?;
        query_one(resp, &a.task_path)
            .and_then(|v| v.as_str().map(str::to_string))
            .filter(|s| !s.is_empty())
    }

    /// Compare an observed answer with the desired values
    fn same(&self, raw: &Value, obj: &Value, vals: &Value) -> bool {
        let desired = query_or_whole(vals, self.cx.values_query.as_deref());
        let observed = match self.cx.read_values_query.as_deref() {
            Some(q) => query_or_whole(raw, Some(q)),
            None => obj.clone(),
        };
        match self.cx.diff {
            DiffMode::None => false,
            DiffMode::Subset => json_subset(&observed, &desired),
            DiffMode::Exact => json_equal(&observed, &desired),
        }
    }

    /// Settle a finished asynchronous task: no write this round, only find the object back
    fn settle(&mut self, owned: &OwnedRestPoint, task: &Value) {
        let mut key = owned.key.clone();
        if key.is_empty()
            && let Some(rp) = self.cx.async_write.as_ref().and_then(|a| a.result_path.as_ref())
            && let Some(k) = query_one(task, rp).and_then(|v| v.as_str().map(str::to_string))
        {
            key = self.decorate(&k);
        }
        let known = if key.is_empty() { None } else { Some(key.as_str()) };
        let mut obj = json!({});
        if let Some(target) = self.read_target(known) {
            obj = self.reread(&target);
            if key.is_empty() {
                match extract_key(&obj, &self.cx.key_name) {
                    Ok(k) => key = self.decorate(&k),
                    Err(m) => self
                        .res
                        .conditions
                        .push(ApplicationCondition::write_failed(&format!(
                            "While finding back write.{}.{} after its task {m}",
                            self.cx.group, self.cx.name
                        ))),
                }
            }
        }
        self.res.obj = obj;
        self.res.key = key;
    }

    /// Poll the task left by a previous reconcile: the item is settled, waiting or failed,
    /// and nothing else is done this round.
    fn follow_task(&mut self, owned: &OwnedRestPoint, task_ref: &str) {
        let Some(a) = self.cx.async_write.clone() else {
            // The async block has been removed from the spec: forget the task
            self.res.key = owned.key.clone();
            return;
        };
        let path = rel_path(task_ref, &self.cx.base_path);
        let task = match self.api.read(self.cx.read_method.clone(), &path, "") {
            Ok(t) => t,
            Err(e) => {
                self.res
                    .conditions
                    .push(ApplicationCondition::reread_failed(&format!(
                        "Reading the task {task_ref} of write.{}.{} raised {e:?}",
                        self.cx.group, self.cx.name
                    )));
                self.res.key = owned.key.clone();
                self.res.pending_task = Some(task_ref.to_string());
                self.res.pending = true;
                return;
            }
        };
        let state = query_one(&task, &a.state_path)
            .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
            .unwrap_or_default();
        if a.done.contains(&state) {
            self.settle(owned, &task);
        } else if a.failed.contains(&state) {
            self.res
                .conditions
                .push(ApplicationCondition::write_failed(&format!(
                    "The task {task_ref} of write.{}.{} ended as {state}: {task}",
                    self.cx.group, self.cx.name
                )));
            self.res.key = owned.key.clone();
        } else {
            self.res
                .conditions
                .push(ApplicationCondition::write_pending(&format!(
                    "write.{}.{} waits for the task {task_ref} ({state})",
                    self.cx.group, self.cx.name
                )));
            self.res.key = owned.key.clone();
            self.res.pending_task = Some(task_ref.to_string());
            self.res.pending = true;
        }
    }

    fn create(&mut self, vals: &Value) {
        let cx = self.cx;
        let static_key = cx.static_key.as_ref().map(|k| self.decorate(k));
        let target = match &cx.static_key {
            Some(k) => format!("{}/{k}", cx.path),
            None if cx.key_use_slash => format!("{}/", cx.path),
            None => cx.path.clone(),
        };
        let mut obj = match self.api.create(cx.create_method.clone(), &target, vals) {
            Ok(o) => o,
            Err(e) => {
                // An empty answer is fine when the key is known or read back
                if !matches!(e, CoreError::JsonError(_)) || (cx.read_path.is_none() && static_key.is_none()) {
                    self.fail("Creating", &e);
                }
                json!({})
            }
        };
        if let Some(task) = self.task_of(&obj) {
            self.res.obj = json!({});
            self.res.key = static_key.unwrap_or_default();
            self.res
                .conditions
                .push(ApplicationCondition::write_pending(&format!(
                    "write.{}.{} waits for the task {task}",
                    cx.group, cx.name
                )));
            self.res.pending_task = Some(task);
            self.res.pending = true;
            return;
        }
        if let Some(rp) = cx.read_path.clone() {
            obj = self.reread(&(rp, String::new()));
        }
        self.res.obj = obj.clone();
        match static_key {
            Some(k) => self.res.key = k,
            None => match extract_key(&obj, &cx.key_name) {
                Ok(k) => self.res.key = self.decorate(&k),
                Err(m) => self
                    .res
                    .conditions
                    .push(ApplicationCondition::write_failed(&format!(
                        "While creating write.{}.{} {m}",
                        cx.group, cx.name
                    ))),
            },
        }
    }

    fn update(&mut self, key: &str, vals: &Value, observed: Option<Value>) {
        let cx = self.cx;
        let (path, k, body) = if cx.key_is_path {
            (key.to_string(), String::new(), vals.clone())
        } else if cx.children_no_sub_key {
            let mut merged = vals.clone();
            merged[cx.key_name.clone()] = json!(key);
            (cx.path.clone(), String::new(), merged)
        } else {
            (
                cx.update_path.clone().unwrap_or(cx.path.clone()),
                key.to_string(),
                vals.clone(),
            )
        };
        let mut obj = match self.api.update(cx.update_method.clone(), &path, &k, &body) {
            Ok(o) => o,
            Err(e) => {
                self.res.giveup = is_404(&e);
                if !matches!(e, CoreError::JsonError(_))
                    || (cx.read_path.is_none() && cx.static_key.is_none())
                {
                    self.fail("Updating", &e);
                }
                json!({})
            }
        };
        self.res.key = key.to_string();
        if let Some(task) = self.task_of(&obj) {
            self.res.obj = observed.unwrap_or_else(|| json!({}));
            self.res
                .conditions
                .push(ApplicationCondition::write_pending(&format!(
                    "write.{}.{} waits for the task {task}",
                    cx.group, cx.name
                )));
            self.res.pending_task = Some(task);
            self.res.pending = true;
            return;
        }
        if let Some(rp) = cx.read_path.clone() {
            obj = self.reread(&(rp, String::new()));
        }
        self.res.obj = obj.clone();
        // The key is known: a new one found in the answer replaces it (e.g. a version used as key)
        if cx.static_key.is_none()
            && !cx.key_is_path
            && let Ok(nk) = extract_key(&obj, &cx.key_name)
        {
            self.res.key = self.decorate(&nk);
        }
    }
}

/// Create, adopt, update or leave alone one item, and follow its asynchronous task
#[must_use]
pub fn process_item<A: ObjApi>(
    api: &mut A,
    cx: &ItemCtx,
    owned: Option<&OwnedRestPoint>,
    vals: &Value,
) -> ItemResult {
    let mut run = Run {
        api,
        cx,
        res: ItemResult {
            obj: json!({}),
            ..ItemResult::default()
        },
    };
    // 1. A task left by a previous reconcile is followed before anything else
    if let Some(o) = owned
        && let Some(t) = o.pending_task.clone()
    {
        run.follow_task(o, &t);
        return run.res;
    }
    // 2. The key we already know
    let mut key: Option<String> = match owned {
        Some(o) if o.key.is_empty() => {
            run.res
                .conditions
                .push(ApplicationCondition::write_failed(&format!(
                    "Will *not* update write.{}.{} with an empty key",
                    cx.group, cx.name
                )));
            return run.res;
        }
        Some(o) => Some(o.key.clone()),
        None => None,
    };
    // 3. Observe the object when it is needed to adopt it or to compare it
    let mut observed: Option<(Value, Value)> = None;
    let static_key = cx.static_key.as_ref().map(|k| run.decorate(k));
    if key.is_none() && cx.adopt {
        let probe = static_key.clone();
        match run.read_target(probe.as_deref()) {
            None => run
                .res
                .conditions
                .push(ApplicationCondition::write_failed(&format!(
                    "write.{}.{} cannot be adopted: set readPath or a key",
                    cx.group, cx.name
                ))),
            Some(t) => match run.read(&t) {
                Ok(Some((raw, obj))) => {
                    let found = match &probe {
                        Some(k) => Some(k.clone()),
                        None => extract_key(&obj, &cx.key_name).ok().map(|k| run.decorate(&k)),
                    };
                    if found.is_some() {
                        key = found;
                        observed = Some((raw, obj));
                    }
                }
                Ok(None) => {}
                Err(e) => run
                    .res
                    .conditions
                    .push(ApplicationCondition::reread_failed(&format!(
                        "Reading write.{}.{} raised {e:?}",
                        cx.group, cx.name
                    ))),
            },
        }
        if !run.res.conditions.is_empty() {
            return run.res;
        }
    }
    if key.is_some()
        && cx.diff != DiffMode::None
        && observed.is_none()
        && let Some(t) = run.read_target(key.as_deref())
    {
        match run.read(&t) {
            Ok(o) => observed = o,
            Err(e) => {
                run.res
                    .conditions
                    .push(ApplicationCondition::reread_failed(&format!(
                        "Reading write.{}.{} raised {e:?}",
                        cx.group, cx.name
                    )));
                return run.res;
            }
        }
    }
    // 4. Decide, then act
    let same = observed.as_ref().map(|(raw, obj)| run.same(raw, obj, vals));
    match decide(key, &cx.diff, same) {
        Decision::Create => run.create(vals),
        Decision::Skip(k) => {
            run.res.obj = observed.map(|(_, o)| o).unwrap_or_default();
            run.res.key = k;
        }
        Decision::Update(k) => run.update(&k, vals, observed.map(|(_, o)| o)),
    }
    run.res
}

#[cfg(test)]
mod tests {
    use super::*;

    type Handler = Box<dyn FnMut(&str, &str, Option<&Value>) -> vynil_core::Result<Value>>;
    struct Fake {
        calls: Vec<String>,
        handler: Handler,
    }
    impl Fake {
        fn new(h: impl FnMut(&str, &str, Option<&Value>) -> vynil_core::Result<Value> + 'static) -> Fake {
            Fake {
                calls: vec![],
                handler: Box::new(h),
            }
        }

        fn call(&mut self, verb: &str, url: &str, body: Option<&Value>) -> vynil_core::Result<Value> {
            self.calls.push(format!("{verb} {url}"));
            (self.handler)(verb, url, body)
        }
    }
    impl ObjApi for Fake {
        fn read(&mut self, _m: ReadMethod, path: &str, key: &str) -> vynil_core::Result<Value> {
            let u = if key.is_empty() {
                path.to_string()
            } else {
                format!("{path}/{key}")
            };
            self.call("GET", &u, None)
        }

        fn create(&mut self, m: CreateMethod, path: &str, input: &Value) -> vynil_core::Result<Value> {
            self.call(
                if m == CreateMethod::Put { "PUT" } else { "POST" },
                path,
                Some(input),
            )
        }

        fn update(
            &mut self,
            m: UpdateMethod,
            path: &str,
            key: &str,
            input: &Value,
        ) -> vynil_core::Result<Value> {
            let u = if key.is_empty() {
                path.to_string()
            } else {
                format!("{path}/{key}")
            };
            let v = match m {
                UpdateMethod::Put => "PUT",
                UpdateMethod::Post => "POST",
                _ => "PATCH",
            };
            self.call(v, &u, Some(input))
        }
    }
    fn not_found() -> CoreError {
        CoreError::MethodFailed("Get".into(), 404, "nope".into())
    }
    fn empty_body() -> CoreError {
        CoreError::JsonError(serde_json::from_str::<Value>("").unwrap_err())
    }
    fn cx() -> ItemCtx {
        ItemCtx {
            group: "g".into(),
            name: "i".into(),
            path: "things".into(),
            update_path: None,
            key_name: "id".into(),
            key_use_slash: false,
            key_is_path: false,
            children_no_sub_key: false,
            static_key: None,
            adopt: false,
            diff: DiffMode::None,
            values_query: None,
            read_values_query: None,
            read_path: None,
            read_json_query: None,
            create_method: CreateMethod::Post,
            read_method: ReadMethod::Get,
            update_method: UpdateMethod::Patch,
            async_write: None,
            base_path: String::new(),
        }
    }
    fn owned(key: &str, task: Option<&str>) -> OwnedRestPoint {
        OwnedRestPoint::new("things", key, "g", "i", true, false, "id")
            .with_pending_task(task.map(str::to_string))
    }
    fn failed(r: &ItemResult) -> bool {
        r.conditions
            .iter()
            .any(|c| c.condition_type != crate::restendpoint::ConditionsType::WritePending)
    }

    #[test]
    fn subset_and_equal() {
        let o = json!({"a": 1, "b": {"c": "x", "d": [1, 2]}, "e": null});
        assert!(json_subset(&o, &json!({"a": 1.0, "b": {"c": "x"}})));
        assert!(!json_subset(&o, &json!({"b": {"d": [1]}})));
        assert!(json_subset(&o, &json!({"missing": null})));
        assert!(!json_subset(&o, &json!({"a": 2})));
        assert!(!json_equal(
            &o,
            &json!({"a": 1, "b": {"c": "x", "d": [1, 2]}, "extra": 1})
        ));
        assert!(json_equal(&o, &json!({"a": 1, "b": {"c": "x", "d": [1, 2]}})));
        assert!(!json_equal(&json!({"a": 1, "z": 2}), &json!({"a": 1})));
    }

    #[test]
    fn paths() {
        assert_eq!(base_path_of("http://pulp:24817/pulp/api/v3"), "/pulp/api/v3");
        assert_eq!(base_path_of("https://vault.example.com"), "");
        assert_eq!(base_path_of("https://v.example.com/v1/"), "/v1");
        let b = "/pulp/api/v3";
        assert_eq!(rel_path("/pulp/api/v3/remotes/file/1/", b), "remotes/file/1/");
        assert_eq!(rel_path("http://h/pulp/api/v3/tasks/2/", b), "tasks/2/");
        assert_eq!(rel_path("/other/x/", b), "other/x/");
        assert_eq!(rel_path("/pulp/api/v3x/y", b), "pulp/api/v3x/y");
        assert_eq!(rel_path("/a/b", ""), "a/b");
    }

    #[test]
    fn decisions() {
        assert_eq!(decide(None, &DiffMode::Subset, None), Decision::Create);
        assert_eq!(
            decide(Some("k".into()), &DiffMode::None, Some(true)),
            Decision::Update("k".into())
        );
        assert_eq!(
            decide(Some("k".into()), &DiffMode::Subset, Some(true)),
            Decision::Skip("k".into())
        );
        assert_eq!(
            decide(Some("k".into()), &DiffMode::Subset, Some(false)),
            Decision::Update("k".into())
        );
        assert_eq!(
            decide(Some("k".into()), &DiffMode::Subset, None),
            Decision::Update("k".into())
        );
    }

    #[test]
    fn plain_create_then_update_keeps_today_behaviour() {
        let mut api = Fake::new(|v, u, _| match (v, u) {
            ("POST", "things") => Ok(json!({"id": 7})),
            _ => Ok(json!({"id": 7})),
        });
        let c = cx();
        let r = process_item(&mut api, &c, None, &json!({"a": 1}));
        assert!(!failed(&r));
        assert_eq!(r.key, "7");
        let o = owned("7", None);
        let r = process_item(&mut api, &c, Some(&o), &json!({"a": 1}));
        assert_eq!(r.key, "7");
        assert_eq!(api.calls, vec!["POST things", "PATCH things/7"]);
    }

    #[test]
    fn key_use_slash_decorates_the_stored_key() {
        let mut api = Fake::new(|_, _, _| Ok(json!({"id": 7})));
        let mut c = cx();
        c.key_use_slash = true;
        let r = process_item(&mut api, &c, None, &json!({}));
        assert_eq!(r.key, "7/");
        assert_eq!(api.calls, vec!["POST things/"]);
    }

    #[test]
    fn static_key_with_empty_204_answers() {
        let mut api = Fake::new(|_, _, _| Err(empty_body()));
        let mut c = cx();
        c.static_key = Some("mypol".into());
        c.create_method = CreateMethod::Put;
        c.update_method = UpdateMethod::Put;
        let r = process_item(&mut api, &c, None, &json!({"policy": "x"}));
        assert!(!failed(&r), "{:?}", r.conditions);
        assert_eq!(r.key, "mypol");
        let o = owned("mypol", None);
        let r = process_item(&mut api, &c, Some(&o), &json!({"policy": "x"}));
        assert!(!failed(&r));
        assert_eq!(r.key, "mypol");
        assert_eq!(api.calls, vec!["PUT things/mypol", "PUT things/mypol"]);
    }

    #[test]
    fn key_is_path_updates_the_href() {
        let mut api = Fake::new(|_, _, _| Ok(json!({"pulp_href": "/pulp/api/v3/remotes/file/1/"})));
        let mut c = cx();
        c.key_is_path = true;
        c.key_name = "pulp_href".into();
        c.base_path = "/pulp/api/v3".into();
        c.path = "remotes/file".into();
        let r = process_item(&mut api, &c, None, &json!({"name": "demo"}));
        assert_eq!(r.key, "remotes/file/1/");
        let o = owned(&r.key, None);
        let r = process_item(&mut api, &c, Some(&o), &json!({"name": "demo"}));
        assert!(!failed(&r), "{:?}", r.conditions);
        assert_eq!(r.key, "remotes/file/1/");
        assert_eq!(api.calls, vec!["POST remotes/file", "PATCH remotes/file/1/"]);
    }

    #[test]
    fn diff_skips_unchanged_and_updates_changed() {
        let mut api = Fake::new(|_, _, _| Ok(json!({"id": 1, "a": 1, "extra": true})));
        let mut c = cx();
        c.diff = DiffMode::Subset;
        let o = owned("1", None);
        let r = process_item(&mut api, &c, Some(&o), &json!({"a": 1}));
        assert_eq!(api.calls, vec!["GET things/1"]);
        assert_eq!(r.key, "1");
        assert_eq!(r.obj["extra"], json!(true));
        let r = process_item(&mut api, &c, Some(&o), &json!({"a": 2}));
        assert_eq!(api.calls, vec!["GET things/1", "GET things/1", "PATCH things/1"]);
        assert!(!failed(&r));
        c.diff = DiffMode::Exact;
        let _ = process_item(&mut api, &c, Some(&o), &json!({"a": 1}));
        assert_eq!(api.calls.last().unwrap(), "PATCH things/1");
    }

    #[test]
    fn diff_compares_different_shapes_for_vault() {
        let mut api = Fake::new(|v, _, _| match v {
            "GET" => Ok(json!({"data": {"data": {"k": "v"}, "metadata": {"version": 3}}})),
            _ => Err(empty_body()),
        });
        let mut c = cx();
        c.static_key = Some("claude".into());
        c.path = "ns/data/home".into();
        c.diff = DiffMode::Subset;
        c.values_query = Some("$.data".into());
        c.read_values_query = Some("$.data.data".into());
        c.create_method = CreateMethod::Put;
        c.update_method = UpdateMethod::Put;
        let o = owned("claude", None);
        let r = process_item(&mut api, &c, Some(&o), &json!({"data": {"k": "v"}}));
        assert_eq!(api.calls, vec!["GET ns/data/home/claude"]);
        assert!(!failed(&r));
        let _ = process_item(&mut api, &c, Some(&o), &json!({"data": {"k": "other"}}));
        assert_eq!(api.calls.last().unwrap(), "PUT ns/data/home/claude");
    }

    #[test]
    fn adopt_an_existing_object_instead_of_creating() {
        let mut api = Fake::new(|_, _, _| Ok(json!({"results": [{"id": 5, "name": "demo"}]})));
        let mut c = cx();
        c.adopt = true;
        c.read_path = Some("things/?name=demo".into());
        c.read_json_query = Some("$.results[0]".into());
        let r = process_item(&mut api, &c, None, &json!({"name": "demo"}));
        assert!(!failed(&r), "{:?}", r.conditions);
        assert_eq!(r.key, "5");
        assert_eq!(api.calls[0], "GET things/?name=demo");
        assert_eq!(api.calls[1], "PATCH things/5");
        assert!(!api.calls.iter().any(|c| c.starts_with("POST")));
    }

    #[test]
    fn adopt_creates_when_nothing_is_found() {
        let mut created = false;
        let mut api = Fake::new(move |v, _, _| match v {
            "GET" if created => Ok(json!({"count": 1, "results": [{"id": 9}]})),
            "GET" => Ok(json!({"count": 0, "results": []})),
            _ => {
                created = true;
                Ok(json!({"id": 9}))
            }
        });
        let mut c = cx();
        c.adopt = true;
        c.read_path = Some("things/?name=demo".into());
        c.read_json_query = Some("$.results[0]".into());
        let r = process_item(&mut api, &c, None, &json!({"name": "demo"}));
        assert_eq!(api.calls, vec![
            "GET things/?name=demo",
            "POST things",
            "GET things/?name=demo"
        ]);
        assert!(!failed(&r), "{:?}", r.conditions);
        assert_eq!(r.key, "9");
        // 404 means absent too
        let mut created = false;
        let mut api = Fake::new(move |v, _, _| match v {
            "GET" if created => Ok(json!({"results": [{"id": 9}]})),
            "GET" => Err(not_found()),
            _ => {
                created = true;
                Ok(json!({"id": 9}))
            }
        });
        let r = process_item(&mut api, &c, None, &json!({"name": "demo"}));
        assert_eq!(api.calls[1], "POST things");
        assert_eq!(r.key, "9");
    }

    #[test]
    fn adopt_without_a_way_to_read_is_an_error() {
        let mut api = Fake::new(|_, _, _| Ok(json!({})));
        let mut c = cx();
        c.adopt = true;
        let r = process_item(&mut api, &c, None, &json!({}));
        assert!(failed(&r));
        assert!(api.calls.is_empty());
    }

    fn async_cx() -> ItemCtx {
        let mut c = cx();
        c.async_write = Some(AsyncWrite {
            task_path: "$.task".into(),
            state_path: "$.state".into(),
            done: vec!["completed".into()],
            failed: vec!["failed".into(), "canceled".into()],
            result_path: Some("$.created_resources[0]".into()),
        });
        c.key_is_path = true;
        c.key_name = "pulp_href".into();
        c.base_path = "/pulp/api/v3".into();
        c
    }

    #[test]
    fn async_create_is_pending_then_followed_then_settled() {
        let c = async_cx();
        let mut api = Fake::new(|_, _, _| Ok(json!({"task": "/pulp/api/v3/tasks/t1/"})));
        let r = process_item(&mut api, &c, None, &json!({"name": "d"}));
        assert!(r.pending);
        assert!(!failed(&r));
        assert_eq!(r.pending_task.as_deref(), Some("/pulp/api/v3/tasks/t1/"));
        assert_eq!(r.key, "");
        // still running: no new POST
        let o = owned("", r.pending_task.as_deref());
        let mut api = Fake::new(|_, _, _| Ok(json!({"state": "running"})));
        let r = process_item(&mut api, &c, Some(&o), &json!({"name": "d"}));
        assert!(r.pending && !failed(&r));
        assert_eq!(api.calls, vec!["GET tasks/t1/"]);
        // finished: the key comes from created_resources, nothing is written
        let mut api = Fake::new(|_, u, _| match u {
            "tasks/t1/" => {
                Ok(json!({"state": "completed", "created_resources": ["/pulp/api/v3/distributions/9/"]}))
            }
            _ => Ok(json!({"pulp_href": "/pulp/api/v3/distributions/9/", "name": "d"})),
        });
        let r = process_item(&mut api, &c, Some(&o), &json!({"name": "d"}));
        assert!(!r.pending && !failed(&r), "{:?}", r.conditions);
        assert_eq!(r.key, "distributions/9/");
        assert_eq!(r.pending_task, None);
        assert!(api.calls.iter().all(|c| c.starts_with("GET")));
    }

    #[test]
    fn async_update_and_failed_task() {
        let c = async_cx();
        let o = owned("remotes/file/1/", None);
        let mut api = Fake::new(|_, _, _| Ok(json!({"task": "/pulp/api/v3/tasks/t2/"})));
        let r = process_item(&mut api, &c, Some(&o), &json!({"name": "d"}));
        assert!(r.pending);
        assert_eq!(r.key, "remotes/file/1/");
        assert_eq!(api.calls, vec!["PATCH remotes/file/1/"]);
        let o = owned("remotes/file/1/", Some("/pulp/api/v3/tasks/t2/"));
        let mut api = Fake::new(|_, _, _| Ok(json!({"state": "failed", "error": {"description": "boom"}})));
        let r = process_item(&mut api, &c, Some(&o), &json!({"name": "d"}));
        assert!(!r.pending && failed(&r));
        assert!(r.conditions[0].message.contains("boom"));
        assert_eq!(r.key, "remotes/file/1/");
    }

    #[test]
    fn update_on_404_gives_up() {
        let mut api = Fake::new(|_, _, _| Err(CoreError::MethodFailed("Patch".into(), 404, "gone".into())));
        let r = process_item(&mut api, &cx(), Some(&owned("1", None)), &json!({}));
        assert!(r.giveup);
    }

    #[test]
    fn empty_owned_key_is_refused() {
        let mut api = Fake::new(|_, _, _| Ok(json!({})));
        let r = process_item(&mut api, &cx(), Some(&owned("", None)), &json!({}));
        assert!(failed(&r));
        assert!(api.calls.is_empty());
    }
}
