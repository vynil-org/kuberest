use crate::{
    Error, Metrics, Result, create, handlebarshandler,
    k8shandlers::{ConfigMapHandler, SecretHandler},
    passwordhandler::Passwords,
    rhaihandler::Script,
    telemetry, template, update,
    writes::{self, AsyncWrite, DiffMode, ItemCtx},
};
use chrono::{self, DateTime, Utc};
use futures::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, ObjectReference, Secret};
use kube::{
    CustomResource, Resource,
    api::{Api, ListParams, Patch, PatchParams, ResourceExt},
    client::Client,
    core::PartialObjectMeta,
    runtime::{
        WatchStreamExt,
        controller::{Action, Controller},
        events::{Event, EventType, Recorder, Reporter},
        finalizer::{Event as Finalizer, finalizer},
        reflector::{ObjectRef, Store},
        watcher::{Config, metadata_watcher},
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json_path::JsonPath;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::{
    runtime::Handle,
    sync::RwLock,
    time::{Duration, Instant},
};
use tracing::*;
use vynil_core::http::{CreateMethod, DeleteMethod, ReadMethod, RestClient, UpdateMethod};

pub static RESTPATH_FINALIZER: &str = "restendpoints.kuberest.solidite.fr";
pub fn get_client_name() -> String {
    RESTPATH_FINALIZER.to_string()
}
/// Delay before a failed reconcile is tried again
const ERROR_RETRY: Duration = Duration::from_secs(5 * 60);
/// Longest delay before the first retry, when the first reconcile could not read its inputs
const FIRST_RUN_RETRY: u64 = 60;
/// Pause before a failed teardown is reported: the report itself triggers the next attempt
const TEARDOWN_BACKOFF: Duration = Duration::from_secs(if cfg!(test) { 0 } else { 30 });

/// ConfigMapRef describe a data input for handlebars renders from a ConfigMap
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
pub struct ConfigMapRef {
    /// Name of the ConfigMap
    pub name: String,
    /// Namespace of the ConfigMap, only used if the cross-namespace option is enabled (default: current object namespace)
    pub namespace: Option<String>,
    /// Is the ConfigMap requiered for processing ? (default: false)
    pub optional: Option<bool>,
}

/// SecretRef describe a data input for handlebars renders from a Secret
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
pub struct SecretRef {
    /// Name of the Secret
    pub name: String,
    /// Namespace of the Secret, only used if the cross-namespace option is enabled (default: current object namespace)
    pub namespace: Option<String>,
    /// Is the Secret optional for processing ? (default: false)
    pub optional: Option<bool>,
}

/// randomPassword describe the rules to generate a password
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RandomPassword {
    /// length of the password (default: 32)
    pub length: Option<u32>,
    /// weight of alpha caracters (default: 60)
    pub weight_alphas: Option<u32>,
    /// weight of numbers caracters (default: 20)
    pub weight_numbers: Option<u32>,
    /// weight of symbols caracters (default: 20)
    pub weight_symbols: Option<u32>,
}

// TODO: ssh-key (priv+pub) as input (for secret generation)

/// inputItem describe a data input for handlebars renders
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InputItem {
    /// name of the input (used for handlebars renders)
    pub name: String,
    /// The ConfigMap to select from
    pub config_map_ref: Option<ConfigMapRef>,
    /// The Secret to select from
    pub secret_ref: Option<SecretRef>,
    /// an handlebars template to be rendered
    pub handle_bars_render: Option<String>,
    /// A password generator
    pub password_generator: Option<RandomPassword>,
}

/// templateItem describe a list of handlebars templates that will be registered with given name
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TemplateItem {
    /// name of the input (used for handlebars renders)
    pub name: String,
    /// The template to register
    pub template: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebClient {
    /// The baseurl the client will use. All path will use this as a prefix
    pub baseurl: String,
    /// Headers to use on each requests to the endpoint
    pub headers: Option<HashMap<String, String>>,
    /// keyName: the key of the object (default: id)
    pub key_name: Option<String>,
    /// Method to use when creating an object (default: Post)
    pub create_method: Option<CreateMethod>,
    /// Method to use when reading an object (default: Get). `Post` (empty body) is for `reads` groups only: `writes` always check existence with `Get`
    pub read_method: Option<ReadMethod>,
    /// Method to use when updating an object (default: Patch)
    pub update_method: Option<UpdateMethod>,
    /// Method to use when deleting an object (default: Delete)
    pub delete_method: Option<DeleteMethod>,
    /// Delete the Objects on RestEndPoint deletion (default: true, inability to do so will block RestEndPoint)
    teardown: Option<bool>,
    /// For self-signed Certificates on the destination endpoint
    pub server_ca: Option<String>,
    /// mTLS client key
    pub client_key: Option<String>,
    /// mTLS client certificate
    pub client_cert: Option<String>,
    /// If true, updates and deletes use the same path as create and include the primary key in the payload (default: false)
    pub children_no_sub_key: Option<bool>,
    /// If true, the key returned by the API is a full path (href) used as is, relative to the baseurl, for updates, deletes and tasks (default: false)
    pub key_is_path: Option<bool>,
}
impl Default for WebClient {
    fn default() -> Self {
        WebClient {
            baseurl: "http://localhost:8080".to_string(),
            headers: None,
            key_name: Some("id".to_string()),
            create_method: Some(CreateMethod::Post),
            read_method: Some(ReadMethod::Get),
            update_method: Some(UpdateMethod::Put),
            delete_method: Some(DeleteMethod::Delete),
            teardown: Some(true),
            server_ca: None,
            client_cert: None,
            client_key: None,
            children_no_sub_key: Some(false),
            key_is_path: None,
        }
    }
}

/// readGroupItem describe an object to read with the client
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
pub struct ReadGroupItem {
    /// name of the item (used for handlebars renders)
    pub name: String,
    /// configuration of this object
    pub key: String,
    /// Allow missing object (default false)
    pub optional: Option<bool>,
    /// Get the result from a json-query
    pub json_query: Option<String>,
}
/// ReadGroup describe a rest endpoint within the client sub-paths,
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
pub struct ReadGroup {
    /// name of the write (used for handlebars renders)
    pub name: String,
    /// path appended to the client's baseurl for this group of objects
    pub path: String,
    /// Method to use when reading an object (default: Get, or `Post` with an empty body)
    pub read_method: Option<ReadMethod>,
    /// The list of object mapping
    pub items: Vec<ReadGroupItem>,
}

/// writeGroupItem describe an object to maintain within
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WriteGroupItem {
    /// name of the item (used for handlebars renders: write.<group>.<name>)
    pub name: String,
    /// configuration of this object (yaml format, use handlebars to generate your needed values)
    pub values: String,
    /// Delete the Object on RestEndPoint deletion (default: true, inability to do so will block RestEndPoint)
    pub teardown: Option<bool>,
    /// If writes doesnt return values, use this read query to re-read
    pub read_path: Option<String>,
    /// If writes doesnt return values, (only used when readPath is specified too)
    pub read_json_query: Option<String>,
    /// Static key (handlebars): the object lives at `<path>/<key>`; create, update and delete use that url and no key is read from the answers
    pub key: Option<String>,
    /// Adopt a pre-existing object (found through readPath, or at `<path>/<key>`) instead of creating it. An existing object answering with an empty body is seen as absent and created (default: group value or false)
    pub adopt: Option<bool>,
    /// Compare the observed object with the values before updating it (default: group value or None)
    pub diff: Option<DiffMode>,
    /// JSON path applied on the rendered values before the comparison (diff). An invalid path is an error;
    /// a path matching nothing (or several values) compares the whole value, i.e. the object is updated
    pub values_query: Option<String>,
    /// JSON path applied on the observed answer before the comparison (diff). Default: the readJsonQuery result
    pub read_values_query: Option<String>,
}
/// writeGroup describe a rest endpoint within the client sub-paths,
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WriteGroup {
    /// name of the write (used for handlebars renders: write.<name>)
    pub name: String,
    /// path appended to the client's baseurl for this group of objects
    pub path: String,
    /// keyName: the key of the object (default: id)
    pub key_name: Option<String>,
    /// keyUseSlash: should the update/delete url end with a slash at the end (default: false)
    pub key_use_slash: Option<bool>,
    /// Method to use when creating an object (default: Post)
    pub create_method: Option<CreateMethod>,
    /// Method to use when reading an object (default: Get). `Post` is not used here: the existence check of a write is always a `Get`
    pub read_method: Option<ReadMethod>,
    /// Method to use when updating an object (default: Patch)
    pub update_method: Option<UpdateMethod>,
    /// Path to use to update/delete this write_group
    pub update_path: Option<String>,
    /// Method to use when deleting an object (default: Delete)
    pub delete_method: Option<DeleteMethod>,
    /// The list of object mapping
    pub items: Vec<WriteGroupItem>,
    /// Delete the Objects on RestEndPoint deletion (default: true, inability to do so will block RestEndPoint)
    teardown: Option<bool>,
    /// If true, updates and deletes use the same path as create and include the primary key in the payload (default: client value or false)
    pub children_no_sub_key: Option<bool>,
    /// If true, the key returned by the API is a full path (href) used as is, relative to the baseurl (default: client value or false)
    pub key_is_path: Option<bool>,
    /// Default for the items: adopt a pre-existing object instead of creating it (default: false)
    pub adopt: Option<bool>,
    /// Default for the items: compare the observed object with the values before updating it (default: None)
    pub diff: Option<DiffMode>,
    /// The API answers writes with a task to follow before the object is settled.
    /// Only creates and updates are followed: a DELETE answering with a task is considered done.
    #[serde(rename = "async")]
    pub async_write: Option<AsyncWrite>,
}

/// metadata describe a rest endpoint within the client sub-paths
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
pub struct Metadata {
    /// name of the created object
    pub name: String,
    /// namespace of the created object
    pub namespace: Option<String>,
    /// labels of the objects
    pub labels: Option<HashMap<String, String>>,
    /// annotations of the objects
    pub annotations: Option<HashMap<String, String>>,
}
impl Default for Metadata {
    fn default() -> Self {
        Metadata {
            name: "default".to_string(),
            namespace: None,
            labels: None,
            annotations: None,
        }
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema, Default)]
pub enum Kind {
    #[default]
    Secret,
    ConfigMap,
}

/// outputItem describe an object that will be created/updated after the path objects are all handled
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OutputItem {
    /// Either ConfigMap or Secret
    kind: Kind,
    /// The metadata of the Object (requiered: name)
    metadata: Metadata,
    /// Data of the Output (will be base64-encoded for secret Secrets)
    data: HashMap<String, String>,
    /// Delete the Secret on RestEndPoint deletion (default: true)
    teardown: Option<bool>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema, Default)]
pub enum ConditionsType {
    #[default]
    Ready,
    InputMissing,
    InputFailed,
    TemplateFailed,
    InitScriptFailed,
    PreScriptFailed,
    PostScriptFailed,
    TeardownScriptFailed,
    ReadFailed,
    ReadMissing,
    ReReadFailed,
    WriteFailed,
    WritePending,
    WriteDeleteFailed,
    WriteAlreadyExist,
    OutputFailed,
    OutputDeleteFailed,
    OutputAlreadyExist,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema, Default)]
pub enum ConditionsStatus {
    #[default]
    True,
    False,
}

/// ApplicationCondition contains details about an application condition, which is usually an error or warning
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationCondition {
    /// LastTransitionTime is the time the condition was last observed
    pub last_transition_time: Option<DateTime<Utc>>,
    /// Message contains human-readable message indicating details about condition
    pub message: String,
    /// Type is an application condition type
    #[serde(rename = "type")]
    pub condition_type: ConditionsType,
    /// Status ("True" or "False") describe if the condition is enbled
    pub status: ConditionsStatus,
}
impl ApplicationCondition {
    #[must_use]
    pub fn new(
        message: &str,
        status: ConditionsStatus,
        condition_type: ConditionsType,
    ) -> ApplicationCondition {
        ApplicationCondition {
            last_transition_time: Some(chrono::offset::Utc::now()),
            status,
            condition_type,
            message: message.to_string(),
        }
    }

    pub fn input_missing(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::InputMissing)
    }

    pub fn output_exist(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(
            message,
            ConditionsStatus::True,
            ConditionsType::OutputAlreadyExist,
        )
    }

    pub fn init_script_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::InitScriptFailed)
    }

    pub fn pre_script_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::PreScriptFailed)
    }

    pub fn post_script_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::PostScriptFailed)
    }

    pub fn teardown_script_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(
            message,
            ConditionsStatus::True,
            ConditionsType::TeardownScriptFailed,
        )
    }

    pub fn write_exist(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::WriteAlreadyExist)
    }

    pub fn input_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::InputFailed)
    }

    pub fn template_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::TemplateFailed)
    }

    pub fn read_missing(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::ReadMissing)
    }

    pub fn read_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::ReadFailed)
    }

    pub fn write_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::WriteFailed)
    }

    pub fn reread_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::ReReadFailed)
    }

    pub fn write_pending(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::WritePending)
    }

    pub fn write_delete_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::WriteDeleteFailed)
    }

    pub fn output_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::OutputFailed)
    }

    pub fn output_delete_failed(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(
            message,
            ConditionsStatus::True,
            ConditionsType::OutputDeleteFailed,
        )
    }

    pub fn is_ready(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::True, ConditionsType::Ready)
    }

    pub fn not_ready(message: &str) -> ApplicationCondition {
        ApplicationCondition::new(message, ConditionsStatus::False, ConditionsType::Ready)
    }
}

/// List all owned k8s objects
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OwnedObjects {
    /// Either ConfigMap or Secret
    pub kind: Kind,
    /// name of the owned object
    pub name: String,
    /// namespace of the owned object
    pub namespace: String,
    /// uid of the owned object
    pub uid: String,
}
impl OwnedObjects {
    #[must_use]
    pub fn new(kind: Kind, name: &str, namespace: &str, uid: &str) -> OwnedObjects {
        OwnedObjects {
            kind,
            name: name.to_string(),
            namespace: namespace.to_string(),
            uid: uid.to_string(),
        }
    }

    pub fn secret(name: &str, namespace: &str, uid: &str) -> OwnedObjects {
        OwnedObjects::new(Kind::Secret, name, namespace, uid)
    }

    pub fn configmap(name: &str, namespace: &str, uid: &str) -> OwnedObjects {
        OwnedObjects::new(Kind::ConfigMap, name, namespace, uid)
    }
}

/// List all owned rest objects
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OwnedRestPoint {
    /// Object path within the client
    pub path: String,
    /// Object key
    pub key: String,
    /// Object writeGroup
    pub group: String,
    /// Object name within its writeGroup
    pub name: String,
    /// should we manage this object deletion
    pub teardown: bool,
    /// If true, deletes include the key in the payload instead of the URL
    pub children_no_sub_key: Option<bool>,
    /// Key field name used for payload construction (default: id)
    pub key_name: Option<String>,
    /// If true, the key is a path relative to the baseurl and replaces the path
    pub key_is_path: Option<bool>,
    /// Reference of the asynchronous task still running for this object
    pub pending_task: Option<String>,
    /// When that task has been started (seconds since the epoch)
    pub pending_since: Option<u64>,
}
impl OwnedRestPoint {
    #[must_use]
    pub fn new(
        path: &str,
        key: &str,
        group: &str,
        name: &str,
        teardown: bool,
        children_no_sub_key: bool,
        key_name: &str,
    ) -> OwnedRestPoint {
        OwnedRestPoint {
            path: path.to_string(),
            key: key.to_string(),
            group: group.to_string(),
            name: name.to_string(),
            teardown,
            children_no_sub_key: Some(children_no_sub_key),
            key_name: Some(key_name.to_string()),
            key_is_path: None,
            pending_task: None,
            pending_since: None,
        }
    }

    #[must_use]
    pub fn with_key_is_path(mut self, key_is_path: bool) -> OwnedRestPoint {
        self.key_is_path = Some(key_is_path);
        self
    }

    #[must_use]
    pub fn with_pending_task(mut self, task: Option<String>) -> OwnedRestPoint {
        self.pending_task = task;
        self
    }

    #[must_use]
    pub fn with_pending_since(mut self, since: Option<u64>) -> OwnedRestPoint {
        self.pending_since = since;
        self
    }

    /// The (path, key) pair to give to `obj_delete`
    #[must_use]
    pub fn delete_target(&self) -> (String, String) {
        if self.key_is_path.unwrap_or(false) {
            (self.key.clone(), String::new())
        } else {
            (self.path.clone(), self.key.clone())
        }
    }
}

/// Describe the specification of a RestEndPoint
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[kube(
    kind = "RestEndPoint",
    group = "kuberest.solidite.fr",
    version = "v1",
    namespaced,
    status = "RestEndPointStatus",
    shortname = "rep"
)]
#[kube(doc = "Custom resource representing a RestEndPoint for kuberest")]
#[kube(
    printcolumn = r#"{"name":"baseurl", "type":"string", "description":"Base URL", "jsonPath":".spec.client.baseurl"}"#
)]
#[kube(
    printcolumn = r#"{"name":"last_updated", "type":"date", "description":"Last update date", "format": "date-time", "jsonPath":".status.conditions[?(@.type == 'Ready')].lastTransitionTime"}"#
)]
#[kube(
    printcolumn = r#"{"name":"errors", "type":"string", "description":"Errors", "jsonPath":".status.conditions[?(@.status == 'False')].message"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RestEndPointSpec {
    /// List Handlebars templates to register
    pub templates: Option<Vec<TemplateItem>>,
    /// List input source for Handlebars renders
    pub inputs: Option<Vec<InputItem>>,
    /// A rhai pre-script to setup some complex variables before client setup
    pub init: Option<String>,
    /// Define the how the client should connect to the API endpoint(s)
    pub client: WebClient,
    /// A rhai pre-script to setup some complex variables
    pub pre: Option<String>,
    /// Allow to read some pre-existing objects
    pub reads: Option<Vec<ReadGroup>>,
    /// Sub-paths to the client. Allow to describe the objects to create on the end-point
    pub writes: Option<Vec<WriteGroup>>,
    /// A rhai post-script for final validation if any
    pub post: Option<String>,
    /// Objects (Secret or ConfigMap) to create at the end of the process
    pub outputs: Option<Vec<OutputItem>>,
    /// checkFrequency define the pooling interval (in seconds, default: 3600 aka 1h)
    pub check_frequency: Option<u64>,
    /// retryFrequency define the pooling interval if previous try have failed (in seconds, default: 300 aka 5mn)
    pub retry_frequency: Option<u64>,
    /// watchInputs: reconcile as soon as a Secret or ConfigMap listed in `inputs` is created, changed or deleted, instead of waiting for `checkFrequency`. Writes made by kuberest itself (outputs) wake nobody up (default: false)
    pub watch_inputs: Option<bool>,
    /// A rhai teardown-script for a final cleanup on RestEndPoint deletion
    pub teardown: Option<String>,
}
/// The status object of `RestEndPoint`
#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestEndPointStatus {
    pub conditions: Vec<ApplicationCondition>,
    pub owned: Vec<OwnedObjects>,
    pub owned_target: Vec<OwnedRestPoint>,
    pub generation: i64,
}

// Context for our reconciler
#[derive(Clone)]
pub struct Context {
    /// Kubernetes client
    pub client: Client,
    /// Diagnostics read by the web server
    pub diagnostics: Arc<RwLock<Diagnostics>>,
    /// Prometheus metrics
    pub metrics: Metrics,
    /// RestEndPoints woken up by a change of one of their inputs
    pub wakeups: Wakeups,
}

/// The RestEndPoints (namespace, name) to wake up, because a watched input changed or because a
/// first retry is planned, with the instant from which the wake-up is due.
///
/// A reconcile skips the events that only come from our own status update; a wake-up
/// must go through that guard, hence this shared map between the watchers and `reconcile`.
#[derive(Clone, Default)]
pub struct Wakeups {
    marks: Arc<std::sync::Mutex<HashMap<(String, String), Instant>>>,
    /// The RestEndPoints already reconciled by this process
    seen: Arc<std::sync::Mutex<HashSet<(String, String)>>>,
}
impl Wakeups {
    fn marks(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), Instant>> {
        self.marks.lock().unwrap()
    }

    /// Is it the first time this process meets this RestEndPoint ?
    pub fn first_sight(&self, ns: &str, name: &str) -> bool {
        self.seen
            .lock()
            .unwrap()
            .insert((ns.to_string(), name.to_string()))
    }

    /// Drop everything known about a RestEndPoint which is being deleted
    pub fn remove(&self, ns: &str, name: &str) {
        self.forget(ns, name);
        self.seen
            .lock()
            .unwrap()
            .remove(&(ns.to_string(), name.to_string()));
    }

    /// Wake a RestEndPoint up now
    pub fn insert(&self, ns: &str, name: &str) {
        self.marks()
            .insert((ns.to_string(), name.to_string()), Instant::now());
    }

    /// Wake a RestEndPoint up again after `delay`, unless a wake-up is already recorded
    pub fn retry_after(&self, ns: &str, name: &str, delay: Duration) {
        self.marks()
            .entry((ns.to_string(), name.to_string()))
            .or_insert_with(|| Instant::now() + delay);
    }

    /// Is there a due wake-up for this RestEndPoint ? (clears it)
    pub fn take_due(&self, ns: &str, name: &str) -> bool {
        let key = (ns.to_string(), name.to_string());
        let mut marks = self.marks();
        let due = marks.get(&key).is_some_and(|at| *at <= Instant::now());
        if due {
            marks.remove(&key);
        }
        due
    }

    /// Time left before the recorded wake-up of this RestEndPoint is due, if there is one
    pub fn due_in(&self, ns: &str, name: &str) -> Option<Duration> {
        self.marks()
            .get(&(ns.to_string(), name.to_string()))
            .map(|at| at.saturating_duration_since(Instant::now()))
    }

    /// Drop the wake-up of this RestEndPoint, due or not. Was there one ?
    pub fn forget(&self, ns: &str, name: &str) -> bool {
        self.marks().remove(&(ns.to_string(), name.to_string())).is_some()
    }
}

/// The kind of object an input can be read from, and watched
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputKind {
    Secret,
    ConfigMap,
}

/// The (namespace, name) of the RestEndPoints with `watchInputs` that list the given Secret or ConfigMap in their inputs
pub fn endpoints_using(
    kind: InputKind,
    ns: &str,
    name: &str,
    endpoints: &[Arc<RestEndPoint>],
) -> Vec<(String, String)> {
    endpoints
        .iter()
        .filter(|rep| rep.spec.watch_inputs.unwrap_or(false))
        .filter(|rep| {
            let rep_ns = rep.namespace().unwrap_or_default();
            rep.spec.inputs.iter().flatten().any(|i| match kind {
                InputKind::Secret => i
                    .secret_ref
                    .as_ref()
                    .is_some_and(|r| r.name == name && r.namespace.as_deref().unwrap_or(&rep_ns) == ns),
                InputKind::ConfigMap => i
                    .config_map_ref
                    .as_ref()
                    .is_some_and(|r| r.name == name && r.namespace.as_deref().unwrap_or(&rep_ns) == ns),
            })
        })
        .map(|rep| (rep.namespace().unwrap_or_default(), rep.name_any()))
        .collect()
}

/// Was the last write on this object made by kuberest itself (an output) ?
/// Such an event must not wake anybody up, otherwise an output used as an input would loop.
///
/// `managedFields[].time` has a one second granularity: when somebody else wrote during the same
/// second, we can't tell who was last, so the event is kept.
pub fn written_by_us(meta: &kube::api::ObjectMeta) -> bool {
    let newest = |ours: bool| {
        meta.managed_fields
            .iter()
            .flatten()
            .filter(|f| (f.manager.as_deref() == Some(RESTPATH_FINALIZER)) == ours)
            .map(|f| f.time.clone())
            .max()
    };
    // `None` (no write at all) is lower than any write
    newest(true) > newest(false)
}

#[instrument(skip(ctx, rest_path), fields(trace_id))]
async fn reconcile(rest_path: Arc<RestEndPoint>, ctx: Arc<Context>) -> Result<Action> {
    let trace_id = telemetry::get_trace_id();
    Span::current().record("trace_id", field::display(&trace_id));
    let _timer = ctx.metrics.count_and_measure();
    ctx.diagnostics.write().await.last_event = Utc::now();
    let ns = rest_path.namespace().unwrap();
    let paths: Api<RestEndPoint> = Api::namespaced(ctx.client.clone(), &ns);

    info!("Reconciling RestEndPoint \"{}\" in {}", rest_path.name_any(), ns);
    finalizer(&paths, RESTPATH_FINALIZER, rest_path, |event| async {
        match event {
            Finalizer::Apply(rest_path) => rest_path.reconcile(ctx.clone()).await,
            Finalizer::Cleanup(rest_path) => rest_path.cleanup(ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::FinalizerError(Box::new(e)))
}

fn error_policy(restendpoint: Arc<RestEndPoint>, error: &Error, ctx: Arc<Context>) -> Action {
    warn!("reconcile failed: {:?}", error);
    ctx.metrics.reconcile_failure(&restendpoint, error);
    Action::requeue(ERROR_RETRY)
}

impl RestEndPoint {
    fn owned(&self) -> Vec<OwnedObjects> {
        if let Some(st) = self.status.clone() {
            st.owned
        } else {
            Vec::new()
        }
    }

    fn owned_target(&self) -> Vec<OwnedRestPoint> {
        if let Some(st) = self.status.clone() {
            st.owned_target
        } else {
            Vec::new()
        }
    }

    async fn publish(
        recorder: &Recorder,
        obj_ref: &ObjectReference,
        reason: String,
        note_p: String,
        action: String,
    ) -> Result<()> {
        let mut note = note_p;
        note.truncate(1023);
        match recorder
            .publish(
                &Event {
                    type_: EventType::Normal,
                    reason,
                    note: Some(note),
                    action,
                    secondary: None,
                },
                obj_ref,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => match e {
                kube::Error::Api(src) => {
                    if !src
                        .message
                        .as_str()
                        .contains("unable to create new content in namespace")
                        || !src.message.as_str().contains("being terminated")
                    {
                        tracing::warn!("Ignoring {:?} while sending an event", src);
                    }
                    Ok(())
                }
                _ => Err(Error::KubeError(e)),
            },
        }
    }

    async fn publish_warning(
        recorder: &Recorder,
        obj_ref: &ObjectReference,
        reason: String,
        note_p: String,
        action: String,
    ) -> Result<()> {
        let mut note = note_p;
        note.truncate(1023);
        match recorder
            .publish(
                &Event {
                    type_: EventType::Warning,
                    reason,
                    note: Some(note),
                    action,
                    secondary: None,
                },
                obj_ref,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => match e {
                kube::Error::Api(src) => {
                    if !src
                        .message
                        .as_str()
                        .contains("unable to create new content in namespace")
                        || !src.message.as_str().contains("being terminated")
                    {
                        tracing::warn!("Ignoring {:?} while sending an event", src);
                    }
                    Ok(())
                }
                _ => Err(Error::KubeError(e)),
            },
        }
    }

    async fn get_tenant_namespaces(&self, client: Client) -> Result<Vec<String>> {
        let my_ns = self.metadata.namespace.clone().unwrap();
        let ns_api: Api<Namespace> = Api::all(client);
        let my_ns_meta = ns_api.get_metadata(&my_ns).await.map_err(Error::KubeError)?;
        let label_key =
            std::env::var("TENANT_LABEL").unwrap_or_else(|_| "kuberest.solidite.fr/tenant".to_string());
        let res = vec![my_ns];
        if let Some(labels) = my_ns_meta.metadata.labels.clone()
            && labels.clone().keys().any(|k| k == &label_key)
        {
            let tenant_name = &labels[&label_key];
            let mut lp = ListParams::default();
            lp = lp.labels(format!("{}=={}", label_key, tenant_name).as_str());
            let my_nss = ns_api.list_metadata(&lp).await.map_err(Error::KubeError)?;
            return Ok(my_nss
                .items
                .into_iter()
                .map(|n| n.metadata.name.unwrap())
                .collect());
        }
        Ok(res)
    }

    // Reconcile (for non-finalizer related changes)
    async fn reconcile(&self, ctx: Arc<Context>) -> Result<Action> {
        // Before everything : is this reconcillation because of our last status update ?
        let (my_ns, my_name) = (self.namespace().unwrap_or_default(), self.name_any());
        let changed = ctx.wakeups.take_due(&my_ns, &my_name);
        // Input changes made while the controller was down, or before its store knew this
        // RestEndPoint, woke nobody up: the first reconcile reads the inputs again
        let first = self.spec.watch_inputs == Some(true) && ctx.wakeups.first_sight(&my_ns, &my_name);
        if changed {
            info!("Woken up, reconciling without waiting");
        } else if first {
            info!("First reconcile since the controller started, reading the inputs again");
        }
        let woken = changed || first;
        if let Some(status) = self.status.clone().filter(|_| !woken) {
            // A failed wake-up still to be retried must not be postponed to the next periodic check
            let pending = ctx.wakeups.due_in(&my_ns, &my_name);
            let wait = |secs: u64| {
                let delay = Duration::from_secs(secs);
                Action::requeue(pending.map_or(delay, |p| delay.min(p)))
            };
            let now = chrono::offset::Utc::now();
            if status.generation == self.metadata.generation.unwrap_or(1) {
                let delta: u64 = (now - status.conditions[0].last_transition_time.unwrap_or(now))
                    .num_seconds()
                    .try_into()
                    .unwrap();
                if status
                    .conditions
                    .into_iter()
                    .any(|c| c.status == ConditionsStatus::False && c.condition_type == ConditionsType::Ready)
                {
                    let next = self.spec.retry_frequency.unwrap_or(5 * 60);
                    if delta < next {
                        debug!(
                            "The spec didnt change, only the status (which this code just did), waiting {} more secs",
                            next - delta
                        );
                        return Ok(wait(next - delta));
                    }
                } else {
                    let next = self.spec.check_frequency.unwrap_or(60 * 60);
                    if next > 0 {
                        if delta < next {
                            debug!(
                                "The spec didnt change, only the status (which this code just did), waiting {} more secs",
                                next - delta
                            );
                            return Ok(wait(next - delta));
                        }
                    } else {
                        warn!("No spec change, should have awaited change. Will do.");
                        return Ok(pending.map_or_else(Action::await_change, Action::requeue));
                    }
                }
            }
        }

        // This full reconcile also serves a wake-up which was waiting for its retry
        let woken = ctx.wakeups.forget(&my_ns, &my_name) || woken;
        let result = self.apply_spec(ctx.clone()).await;
        // A failed reconcile may not have recorded anything in the status: keep the wake-up,
        // otherwise the guard above would skip the retry until `checkFrequency`
        if woken && result.is_err() {
            ctx.wakeups.retry_after(&my_ns, &my_name, ERROR_RETRY);
        }
        result
    }

    // Read the inputs, call the REST endpoint and write the outputs
    async fn apply_spec(&self, ctx: Arc<Context>) -> Result<Action> {
        let client = ctx.client.clone();
        let recorder = ctx.diagnostics.read().await.recorder(client.clone());
        let obj_ref = self.object_ref(&());
        let ns = self.namespace().unwrap();
        let name = self.name_any();
        let restendpoints: Api<RestEndPoint> = Api::namespaced(client.clone(), &ns);
        let mut conditions: Vec<ApplicationCondition> = Vec::new();
        let mut values = serde_json::json!({"input":{},"pre":{},"read":{},"write":{},"post":{}});
        let mut hbs = handlebarshandler::new_hbs();
        if let Some(templates) = self.spec.templates.clone() {
            for item in templates {
                hbs.register_template(&item.name, &item.template)
                    .unwrap_or_else(|e| {
                        conditions.push(ApplicationCondition::template_failed(&format!(
                            "Registering template.{} raised {e:?}",
                            item.name
                        )));
                    });
            }
        }
        let mut rhai = Script::new();
        rhai.ctx.set_or_push("hbs", hbs.clone());
        let use_multi = std::env::var("MULTI_TENANT")
            .unwrap_or_else(|_| "true".to_string())
            .to_lowercase()
            == *"true";
        let allowed = if use_multi && (self.spec.inputs.is_some() || self.spec.outputs.is_some()) {
            self.get_tenant_namespaces(client.clone()).await?
        } else {
            Vec::new()
        };

        // Read the inputs first
        if let Some(inputs) = self.spec.clone().inputs {
            for input in inputs {
                if let Some(secret) = input.secret_ref {
                    let my_ns = if use_multi {
                        if let Some(o_ns) = secret.namespace.clone() {
                            if allowed.contains(&o_ns) {
                                o_ns
                            } else {
                                Self::publish_warning(&recorder, &obj_ref, String::from("IgnoredNamespace"), format!("Ignoring namespace from Input '{}' as the operator run in multi-tenant mode",input.name), String::from("readingInput")).await?;
                                ns.clone()
                            }
                        } else {
                            ns.clone()
                        }
                    } else if let Some(local_ns) = secret.namespace {
                        local_ns
                    } else {
                        ns.clone()
                    };
                    let mut secrets = SecretHandler::new(&ctx.client.clone(), &my_ns);
                    match secrets.get(&secret.name).await {
                        Ok(my_secret) => {
                            values["input"][input.name] = serde_json::json!({
                                "metadata": my_secret.metadata,
                                "data": my_secret.data
                            });
                        }
                        Err(e) => {
                            if secret.optional.unwrap_or(false) {
                                let _ = Self::publish(
                                    &recorder,
                                    &obj_ref,
                                    String::from("IgnoredInput"),
                                    format!("Ignoring not found secret for Input '{}'", input.name),
                                    String::from("readingInput"),
                                )
                                .await;
                                warn!(
                                    "Optional secret '{}' not found for Input '{}': {}",
                                    secret.name, input.name, e
                                );
                                conditions.push(ApplicationCondition::input_missing(&format!(
                                    "Input '{}' Secret {}.{} not found",
                                    input.name, my_ns, secret.name
                                )));
                                values["input"][input.name] = serde_json::json!({});
                            } else {
                                let _ = Self::publish(
                                    &recorder,
                                    &obj_ref,
                                    String::from("MissingSecret"),
                                    format!("Secret '{}' not found for Input '{}'", secret.name, input.name),
                                    String::from("readingInput"),
                                )
                                .await;
                                error!(
                                    "Required secret '{}' not found for Input '{}': {}",
                                    secret.name, input.name, e
                                );
                                conditions.push(ApplicationCondition::input_failed(&format!(
                                    "Input '{}' Secret {}.{} not found",
                                    input.name, my_ns, secret.name
                                )));
                            }
                        }
                    }
                } else if let Some(cfgmap) = input.config_map_ref {
                    let my_ns = if use_multi {
                        if let Some(o_ns) = cfgmap.namespace.clone() {
                            if allowed.contains(&o_ns) {
                                o_ns
                            } else {
                                Self::publish_warning(&recorder, &obj_ref, String::from("IgnoredNamespace"), format!("Ignoring namespace from Input '{}' as the operator run in multi-tenant mode",input.name), String::from("readingInput")).await?;
                                ns.clone()
                            }
                        } else {
                            ns.clone()
                        }
                    } else if let Some(local_ns) = cfgmap.namespace {
                        local_ns
                    } else {
                        ns.clone()
                    };
                    let mut maps = ConfigMapHandler::new(&ctx.client.clone(), &my_ns);
                    match maps.get(&cfgmap.name).await {
                        Ok(my_cfg) => {
                            values["input"][input.name] = serde_json::json!({
                                "metadata": my_cfg.metadata,
                                "data": my_cfg.data,
                                "binaryData": my_cfg.binary_data
                            });
                        }
                        Err(e) => {
                            if cfgmap.optional.unwrap_or(false) {
                                let _ = Self::publish(
                                    &recorder,
                                    &obj_ref,
                                    String::from("IgnoredInput"),
                                    format!("Ignoring not found ConfigMap for Input '{}'", input.name),
                                    String::from("readingInput"),
                                )
                                .await;
                                warn!(
                                    "Optional configmap '{}' not found for Input '{}': {}",
                                    cfgmap.name, input.name, e
                                );
                                conditions.push(ApplicationCondition::input_missing(&format!(
                                    "Input '{}' ConfigMap {}.{} not found",
                                    input.name, my_ns, cfgmap.name
                                )));
                                values["input"][input.name] = serde_json::json!({});
                            } else {
                                let _ = Self::publish(
                                    &recorder,
                                    &obj_ref,
                                    String::from("MissingConfigMap"),
                                    format!(
                                        "ConfigMap '{}' not found for Input '{}'",
                                        cfgmap.name, input.name
                                    ),
                                    String::from("readingInput"),
                                )
                                .await;
                                error!(
                                    "Required configmap '{}' not found for Input '{}': {}",
                                    cfgmap.name, input.name, e
                                );
                                conditions.push(ApplicationCondition::input_failed(&format!(
                                    "Input '{}' ConfigMap {}.{} not found",
                                    input.name, my_ns, cfgmap.name
                                )));
                            }
                        }
                    }
                } else if let Some(render) = input.handle_bars_render {
                    values["input"][input.name] = json!(template!(
                        render.as_str(),
                        hbs,
                        &values,
                        conditions,
                        recorder,
                        &obj_ref
                    ));
                } else if let Some(password_def) = input.password_generator {
                    values["input"][input.name] = json!(Passwords::new().generate(
                        password_def.length.unwrap_or(32),
                        password_def.weight_alphas.unwrap_or(60),
                        password_def.weight_numbers.unwrap_or(20),
                        password_def.weight_symbols.unwrap_or(20)
                    ));
                } else {
                    Self::publish_warning(
                        &recorder,
                        &obj_ref,
                        String::from("EmptyInput"),
                        format!("Input '{}' have no source", input.name),
                        String::from("fail"),
                    )
                    .await?;
                    conditions.push(ApplicationCondition::input_failed(&format!(
                        "Input '{}' have no source",
                        input.name
                    )));
                    conditions.push(ApplicationCondition::not_ready(&format!(
                        "Input '{}' have no source",
                        input.name
                    )));
                    let new_status = Patch::Apply(json!({
                        "apiVersion": "kuberest.solidite.fr/v1",
                        "kind": "RestEndPoint",
                        "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: self.owned_target() }
                    }));
                    let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
                    let _o = restendpoints
                        .patch_status(&name, &ps, &new_status)
                        .await
                        .map_err(Error::KubeError)?;
                    return Err(Error::IllegalRestEndPoint);
                }
            }
        }
        // Validate that reading went Ok
        if conditions
            .iter()
            .any(|c| c.condition_type != ConditionsType::InputMissing)
        {
            let msg = "Some input failed";
            Self::publish_warning(
                &recorder,
                &obj_ref,
                String::from(msg),
                format!("Found {} error(s) while handling input", conditions.len()),
                String::from("fail"),
            )
            .await?;
            conditions.push(ApplicationCondition::not_ready(msg));
            let new_status = Patch::Apply(json!({
                "apiVersion": "kuberest.solidite.fr/v1",
                "kind": "RestEndPoint",
                "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: self.owned_target() }
            }));
            let first_run = self.status.is_none();
            let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
            let _o = restendpoints
                .patch_status(&name, &ps, &new_status)
                .await
                .map_err(Error::KubeError)?;
            // Some missing Secret or ConfigMap might be ok on first run, dont wait too much
            if first_run {
                let delay = Duration::from_secs(
                    self.spec
                        .retry_frequency
                        .map_or(FIRST_RUN_RETRY, |r| r.min(FIRST_RUN_RETRY)),
                );
                // Without a wake-up, the guard of reconcile() postpones this retry to retryFrequency
                ctx.wakeups.retry_after(&ns, &name, delay);
                return Ok(Action::requeue(delay));
            } else {
                return Ok(Action::requeue(Duration::from_secs(
                    self.spec.retry_frequency.unwrap_or(5 * 60),
                )));
            }
        }
        rhai.set_dynamic("input", &values["input"]);
        rhai.set_dynamic("values", &values);

        // Run the init script
        if let Some(script) = self.spec.init.clone() {
            let cnd = conditions.clone();
            values["init"] = rhai.eval(&script).unwrap_or_else(|e| {
                conditions.push(ApplicationCondition::init_script_failed(&format!("{e:?}")));
                json!({})
            });
            // Validate that init-script went Ok
            if cnd
                .iter()
                .any(|c| c.condition_type != ConditionsType::InputMissing)
            {
                let msg = "Init-script failed";
                Self::publish_warning(
                    &recorder,
                    &obj_ref,
                    String::from(msg),
                    format!("Found {} error(s) running the init-script", conditions.len()),
                    String::from("fail"),
                )
                .await?;
                conditions.push(ApplicationCondition::not_ready(msg));
                let new_status = Patch::Apply(json!({
                    "apiVersion": "kuberest.solidite.fr/v1",
                    "kind": "RestEndPoint",
                    "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: self.owned_target() }
                }));
                let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
                let _o = restendpoints
                    .patch_status(&name, &ps, &new_status)
                    .await
                    .map_err(Error::KubeError)?;
                return Ok(Action::requeue(Duration::from_secs(
                    self.spec.retry_frequency.unwrap_or(5 * 60),
                )));
            }
        }
        rhai.set_dynamic("init", &values["init"]);
        rhai.set_dynamic("values", &values);

        // Setup the httpClient
        let base_url = template!(
            self.spec.client.baseurl.clone().as_str(),
            hbs,
            &values,
            conditions,
            recorder,
            &obj_ref
        );
        let mut rest = RestClient::new(&base_url);
        if let Some(headers) = self.spec.client.headers.clone() {
            for (key, value) in headers {
                rest.add_header(
                    &template!(key.as_str(), hbs, &values, conditions, recorder, &obj_ref),
                    &template!(value.as_str(), hbs, &values, conditions, recorder, &obj_ref),
                );
            }
        }
        rest.add_header_json();
        if let Some(ca) = self.spec.client.server_ca.clone() {
            rest.set_server_ca(&template!(
                &ca.as_str(),
                hbs,
                &values,
                conditions,
                recorder,
                &obj_ref
            ));
        }
        if self.spec.client.client_cert.is_some() && self.spec.client.client_key.is_some() {
            rest.set_mtls(
                &template!(
                    &self.spec.client.client_cert.clone().unwrap().as_str(),
                    hbs,
                    &values,
                    conditions,
                    recorder,
                    &obj_ref
                ),
                &template!(
                    &self.spec.client.client_key.clone().unwrap().as_str(),
                    hbs,
                    &values,
                    conditions,
                    recorder,
                    &obj_ref
                ),
            );
        }
        // Validate that client setup went Ok
        let cnd = conditions.clone();
        if cnd
            .iter()
            .any(|c| c.condition_type != ConditionsType::InputMissing)
        {
            let msg = "Client setup failed";
            Self::publish_warning(
                &recorder,
                &obj_ref,
                String::from(msg),
                format!("Found {} error(s) while setting up the client", conditions.len()),
                String::from("fail"),
            )
            .await?;
            conditions.push(ApplicationCondition::not_ready(msg));
            let new_status = Patch::Apply(json!({
                "apiVersion": "kuberest.solidite.fr/v1",
                "kind": "RestEndPoint",
                "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: self.owned_target() }
            }));
            let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
            let _o = restendpoints
                .patch_status(&name, &ps, &new_status)
                .await
                .map_err(Error::KubeError)?;
            return Ok(Action::requeue(Duration::from_secs(
                self.spec.retry_frequency.unwrap_or(5 * 60),
            )));
        }
        rhai.ctx.set_or_push("client", rest.clone());

        // Run the pre script
        if let Some(script) = self.spec.pre.clone() {
            let cnd = conditions.clone();
            values["pre"] = rhai.eval(&script).unwrap_or_else(|e| {
                conditions.push(ApplicationCondition::pre_script_failed(&format!("{e:?}")));
                json!({})
            });
            // Validate that pre-script went Ok
            if cnd
                .iter()
                .any(|c| c.condition_type != ConditionsType::InputMissing)
            {
                let msg = "Pre-script failed";
                Self::publish_warning(
                    &recorder,
                    &obj_ref,
                    String::from(msg),
                    format!("Found {} error(s) running the pre-script", conditions.len()),
                    String::from("fail"),
                )
                .await?;
                conditions.push(ApplicationCondition::not_ready(msg));
                let new_status = Patch::Apply(json!({
                    "apiVersion": "kuberest.solidite.fr/v1",
                    "kind": "RestEndPoint",
                    "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: self.owned_target() }
                }));
                let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
                let _o = restendpoints
                    .patch_status(&name, &ps, &new_status)
                    .await
                    .map_err(Error::KubeError)?;
                return Ok(Action::requeue(Duration::from_secs(
                    self.spec.retry_frequency.unwrap_or(5 * 60),
                )));
            }
        }
        rhai.set_dynamic("pre", &values["pre"]);
        rhai.set_dynamic("values", &values);

        // Handle each Reads
        if let Some(reads) = self.spec.reads.clone() {
            for group in reads {
                values["read"][group.name.clone()] = serde_json::json!({});
                let path = template!(group.path.as_str(), hbs, &values, conditions, recorder, &obj_ref);
                for read in group.items {
                    let result =
                        rest.clone()
                            .obj_read(
                                group.read_method.clone().unwrap_or(
                                    self.spec.client.read_method.clone().unwrap_or(ReadMethod::Get),
                                ),
                                path.as_str(),
                                &template!(read.key.as_str(), hbs, &values, conditions, recorder, &obj_ref),
                            )
                            .unwrap_or_else(|e| {
                                if read.optional.unwrap_or(false) {
                                    conditions.push(ApplicationCondition::read_missing(&format!(
                                        "Reading read.{}.{} raised {e:?}",
                                        group.name.clone(),
                                        read.name
                                    )));
                                } else {
                                    conditions.push(ApplicationCondition::read_failed(&format!(
                                        "Reading read.{}.{} raised {e:?}",
                                        group.name.clone(),
                                        read.name
                                    )));
                                }
                                json!({})
                            });
                    let mut obj = result.clone();
                    if let Some(json_path) = read.json_query {
                        let _ = JsonPath::parse(&json_path).map(|path| {
                            obj = path
                                .query(&result)
                                .at_most_one()
                                .unwrap_or(Some(&result))
                                .unwrap_or(&result)
                                .clone();
                        });
                    }
                    values["read"][group.name.clone()][read.name] = obj;
                }
            }
        }
        // Validate that all reads went Ok
        let cnd = conditions.clone();
        if cnd.iter().any(|c| {
            c.condition_type != ConditionsType::InputMissing
                && c.condition_type != ConditionsType::ReadMissing
        }) {
            let msg = "Some read have failed";
            Self::publish_warning(
                &recorder,
                &obj_ref,
                String::from(msg),
                format!("Found {} error(s) while reading values", conditions.len()),
                String::from("fail"),
            )
            .await?;
            conditions.push(ApplicationCondition::not_ready(msg));
            let first_run = self.status.is_none();
            let new_status = Patch::Apply(json!({
                "apiVersion": "kuberest.solidite.fr/v1",
                "kind": "RestEndPoint",
                "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: self.owned_target() }
            }));
            let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
            let _o = restendpoints
                .patch_status(&name, &ps, &new_status)
                .await
                .map_err(Error::KubeError)?;
            // We might be trying to configure an app no yet from the same installation, dont wait too much on first try
            if first_run {
                return Ok(Action::requeue(Duration::from_secs(60)));
            } else {
                return Ok(Action::requeue(Duration::from_secs(
                    self.spec.retry_frequency.unwrap_or(5 * 60),
                )));
            }
        }
        rhai.set_dynamic("read", &values["read"]);
        rhai.set_dynamic("values", &values);

        // Handle each Writes
        let mut target_new: Vec<OwnedRestPoint> = Vec::new();
        let mut failures: i32 = 0;
        // An item waiting for an asynchronous task stops the following ones
        let mut blocked = false;
        let base_path = writes::base_path_of(&base_url);
        if let Some(writes_spec) = self.spec.writes.clone() {
            for group in writes_spec {
                let key_name = group
                    .key_name
                    .clone()
                    .unwrap_or(self.spec.client.key_name.clone().unwrap_or("id".to_string()));
                let children_no_sub_key = group
                    .children_no_sub_key
                    .unwrap_or(self.spec.client.children_no_sub_key.unwrap_or(false));
                let key_is_path = group
                    .key_is_path
                    .unwrap_or(self.spec.client.key_is_path.unwrap_or(false));
                values["write"][group.name.clone()] = serde_json::json!({});
                let path = template!(group.path.as_str(), hbs, &values, conditions, recorder, &obj_ref);
                for item in group.items.clone() {
                    let old = self
                        .owned_target()
                        .into_iter()
                        .find(|t| t.group == group.name && t.name == item.name);
                    if blocked {
                        // Keep what we already own, the pending item comes first
                        if let Some(o) = old {
                            target_new.push(o);
                        }
                        continue;
                    }
                    let cur_len = conditions.len();
                    let vals = serde_yaml::from_str(
                        template!(item.values.as_str(), hbs, &values, conditions, recorder, &obj_ref)
                            .as_str(),
                    )
                    .unwrap_or_else(|e| {
                        conditions.push(ApplicationCondition::write_failed(&format!(
                            "Templating write.{}.{}.values \n{}\nusing values: \n{}\nraised {e:?}",
                            group.name.clone(),
                            item.name,
                            item.values.as_str(),
                            serde_yaml::to_string(&values).unwrap_or_default()
                        )));
                        json!({})
                    });
                    let static_key = item
                        .key
                        .as_ref()
                        .map(|k| template!(k.as_str(), hbs, &values, conditions, recorder, &obj_ref));
                    if conditions.len() != cur_len {
                        // Templating failed: keep what we already own, otherwise the
                        // cleanup below would delete it
                        if let Some(o) = old {
                            target_new.push(o);
                        }
                        continue;
                    }
                    let cx = ItemCtx {
                        group: group.name.clone(),
                        name: item.name.clone(),
                        path: path.clone(),
                        update_path: group.update_path.clone(),
                        key_name: key_name.clone(),
                        key_use_slash: group.key_use_slash.unwrap_or(false),
                        key_is_path,
                        children_no_sub_key,
                        static_key,
                        adopt: item.adopt.or(group.adopt).unwrap_or(false),
                        diff: item.diff.clone().or(group.diff.clone()).unwrap_or_default(),
                        values_query: item.values_query.clone(),
                        read_values_query: item.read_values_query.clone(),
                        read_path: item.read_path.clone(),
                        read_json_query: item.read_json_query.clone(),
                        create_method: group.create_method.clone().unwrap_or(
                            self.spec
                                .client
                                .create_method
                                .clone()
                                .unwrap_or(CreateMethod::Post),
                        ),
                        // the existence check of a write must be a side-effect free read:
                        // `Post` is only meant for `reads` groups
                        read_method: match group
                            .read_method
                            .clone()
                            .unwrap_or(self.spec.client.read_method.clone().unwrap_or(ReadMethod::Get))
                        {
                            ReadMethod::Post => ReadMethod::Get,
                            m => m,
                        },
                        update_method: group.update_method.clone().unwrap_or(
                            self.spec
                                .client
                                .update_method
                                .clone()
                                .unwrap_or(UpdateMethod::Patch),
                        ),
                        async_write: group.async_write.clone(),
                        base_path: base_path.clone(),
                        now: chrono::Utc::now().timestamp().unsigned_abs(),
                    };
                    let mut api = rest.clone();
                    let res = writes::process_item(&mut api, &cx, old.as_ref(), &vals);
                    values["write"][group.name.clone()][item.name.clone()] = res.obj.clone();
                    conditions.extend(res.conditions);
                    if res.pending {
                        blocked = true;
                    }
                    if res.giveup {
                        failures += 1;
                    } else if !res.key.is_empty() || res.pending_task.is_some() {
                        let owned_path = if children_no_sub_key {
                            path.clone()
                        } else {
                            group.update_path.clone().unwrap_or(path.clone())
                        };
                        target_new.push(
                            OwnedRestPoint::new(
                                owned_path.as_str(),
                                res.key.as_str(),
                                group.name.as_str(),
                                item.name.as_str(),
                                item.teardown.unwrap_or(
                                    group
                                        .teardown
                                        .unwrap_or(self.spec.client.teardown.unwrap_or(true)),
                                ),
                                children_no_sub_key,
                                key_name.as_str(),
                            )
                            .with_key_is_path(key_is_path)
                            .with_pending_task(res.pending_task)
                            .with_pending_since(res.pending_since),
                        );
                    }
                }
            }
        }
        // Delete old owned writes if no failures
        if failures == 0 && !blocked {
            for old in self.owned_target().clone() {
                if old.teardown
                    && !target_new
                        .clone()
                        .into_iter()
                        .any(|t| t.group == old.group && t.name == old.name)
                {
                    let delete_result = if old.children_no_sub_key.unwrap_or(false) {
                        let del_key_name = old.key_name.clone().unwrap_or("id".to_string());
                        rest.obj_delete_with_body(
                            DeleteMethod::Delete,
                            &old.path,
                            &json!({del_key_name: old.key}),
                        )
                    } else {
                        {
                            let (dp, dk) = old.delete_target();
                            rest.obj_delete(DeleteMethod::Delete, &dp, &dk)
                        }
                    };
                    delete_result.unwrap_or_else(|e| {
                        let giveup = if let vynil_core::Error::MethodFailed(_, code, _) = e {
                            code == 404
                        } else {
                            false
                        };
                        // Allow the user to quit the finalizer loop by setting spec.client.teardown to true
                        if !self.spec.client.teardown.unwrap_or(false) && !giveup {
                            target_new.push(old.clone());
                            conditions.push(ApplicationCondition::write_delete_failed(
                                format!(
                                    "Deleting write.{}.{} {e:?}",
                                    old.group.as_str(),
                                    old.name.as_str()
                                )
                                .as_str(),
                            ));
                        }
                        target_new.push(old.clone());
                        tokio::task::block_in_place(|| {
                            Handle::current().block_on(async {
                                Self::publish_warning(
                                    &recorder,
                                    &obj_ref,
                                    format!("Failed deleting: write.{}.{}", old.group, old.name),
                                    format!("{e:?}"),
                                    String::from("deleting"),
                                )
                                .await
                                .unwrap_or(());
                            })
                        });
                        json!({})
                    });
                }
            }
        } else {
            warn!("Write failures detected ({}), no cleanup processed.", failures);
        }
        // Verify that all writes went OK
        let cnd = conditions.clone();
        if cnd.iter().any(|c| {
            c.condition_type != ConditionsType::InputMissing
                && c.condition_type != ConditionsType::ReadMissing
                && c.condition_type != ConditionsType::WriteAlreadyExist
        }) {
            let only_pending = cnd.iter().all(|c| {
                c.condition_type == ConditionsType::WritePending
                    || c.condition_type == ConditionsType::InputMissing
                    || c.condition_type == ConditionsType::ReadMissing
                    || c.condition_type == ConditionsType::WriteAlreadyExist
            });
            if only_pending {
                let msg = "Some writes are waiting for a task";
                Self::publish(
                    &recorder,
                    &obj_ref,
                    String::from(msg),
                    format!("{} write(s) pending", conditions.len()),
                    String::from("pending"),
                )
                .await?;
                conditions.push(ApplicationCondition::not_ready(msg));
            } else {
                let msg = "Some writes have failed";
                Self::publish_warning(
                    &recorder,
                    &obj_ref,
                    String::from(msg),
                    format!("Found {} error(s) while writing values", conditions.len()),
                    String::from("fail"),
                )
                .await?;
                conditions.push(ApplicationCondition::not_ready(msg));
            }
            let new_status = Patch::Apply(json!({
                "apiVersion": "kuberest.solidite.fr/v1",
                "kind": "RestEndPoint",
                "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: target_new }
            }));
            let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
            let _o = restendpoints
                .patch_status(&name, &ps, &new_status)
                .await
                .map_err(Error::KubeError)?;
            return Ok(Action::requeue(Duration::from_secs(
                self.spec.retry_frequency.unwrap_or(5 * 60),
            )));
        }
        rhai.set_dynamic("write", &values["write"]);
        rhai.set_dynamic("values", &values);

        // Run the post script
        if let Some(script) = self.spec.post.clone() {
            values["post"] = rhai.eval(&script).unwrap_or_else(|e| {
                conditions.push(ApplicationCondition::post_script_failed(&format!("{e:?}")));
                json!({})
            });
            // Validate that post-script went Ok
            let cnd = conditions.clone();
            if cnd.iter().any(|c| {
                c.condition_type != ConditionsType::InputMissing
                    && c.condition_type != ConditionsType::ReadMissing
            }) {
                let msg = "Post-script failed";
                Self::publish_warning(
                    &recorder,
                    &obj_ref,
                    String::from(msg),
                    format!("Found {} error(s) during post-script", conditions.len()),
                    String::from("fail"),
                )
                .await?;
                conditions.push(ApplicationCondition::not_ready(msg));
                let new_status = Patch::Apply(json!({
                    "apiVersion": "kuberest.solidite.fr/v1",
                    "kind": "RestEndPoint",
                    "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: target_new }
                }));
                let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
                let _o = restendpoints
                    .patch_status(&name, &ps, &new_status)
                    .await
                    .map_err(Error::KubeError)?;
                return Ok(Action::requeue(Duration::from_secs(
                    self.spec.retry_frequency.unwrap_or(5 * 60),
                )));
            }
        }

        // Finally: handle Outputs
        let mut owned_new: Vec<OwnedObjects> = Vec::new();
        if let Some(outputs) = self.spec.outputs.clone() {
            for output in outputs {
                let my_ns = if use_multi {
                    if let Some(o_ns) = output.metadata.namespace.clone() {
                        if allowed.contains(&o_ns) {
                            o_ns
                        } else {
                            Self::publish_warning(&recorder, &obj_ref, String::from("IgnoredNamespace"), format!("Ignoring namespace from Input '{}' as the operator run in multi-tenant mode",output.metadata.name), String::from("readingOutput")).await?;
                            ns.clone()
                        }
                    } else {
                        ns.clone()
                    }
                } else if let Some(local_ns) = output.clone().metadata.namespace {
                    local_ns
                } else {
                    ns.clone()
                };
                let mut my_values: HashMap<String, String> = HashMap::new();
                for (key, val) in output.clone().data {
                    my_values.insert(
                        template!(key.as_str(), hbs, &values, conditions, recorder, &obj_ref),
                        template!(val.as_str(), hbs, &values, conditions, recorder, &obj_ref),
                    );
                }
                if output.kind == Kind::Secret {
                    let mut secrets = SecretHandler::new(&ctx.client.clone(), &my_ns);
                    if self.owned().clone().into_iter().any(|o| {
                        o.kind == output.kind && o.name == output.metadata.name && o.namespace == my_ns
                    }) {
                        let myself = self
                            .owned()
                            .clone()
                            .into_iter()
                            .find(|o| {
                                o.kind == output.kind
                                    && o.name == output.metadata.name
                                    && o.namespace == my_ns
                            })
                            .unwrap();
                        if secrets.have_uid(&myself.name, &myself.uid).await {
                            if !secrets.have_with_data(&myself.name, &my_values).await {
                                let _ = update!(
                                    secrets, "Secret", output, my_ns, &my_values, conditions, recorder,
                                    &obj_ref
                                );
                            }
                            owned_new.push(myself);
                        } else if secrets.have(&myself.name).await {
                            if !secrets.have_with_data(&myself.name, &my_values).await {
                                let _ = update!(
                                    secrets, "Secret", output, my_ns, &my_values, conditions, recorder,
                                    &obj_ref
                                );
                            }
                            if output.teardown.unwrap_or(true) {
                                conditions.push(ApplicationCondition::output_exist(&format!(
                                    "Secret output '{}.{}' already exist, won't take ownership.",
                                    my_ns, output.metadata.name
                                )));
                            }
                        } else {
                            let own = if output.teardown.unwrap_or(true)
                                && my_ns == self.namespace().unwrap_or(my_ns.clone())
                            {
                                Some(self)
                            } else {
                                None
                            };
                            if let Some(sec) = create!(
                                secrets, "Secret", own, output, my_ns, &my_values, conditions, recorder,
                                &obj_ref
                            ) && output.teardown.unwrap_or(true)
                            {
                                owned_new.push(OwnedObjects::secret(
                                    sec.metadata.name.unwrap().as_str(),
                                    sec.metadata.namespace.unwrap().as_str(),
                                    sec.metadata.uid.unwrap().as_str(),
                                ));
                            }
                        }
                    } else if secrets.have(&output.metadata.name).await {
                        if !secrets.have_with_data(&output.metadata.name, &my_values).await {
                            let _ = update!(
                                secrets, "Secret", output, my_ns, &my_values, conditions, recorder, &obj_ref
                            );
                        }
                        if output.teardown.unwrap_or(true) {
                            conditions.push(ApplicationCondition::output_exist(&format!(
                                "Secret output '{}.{}' already exist, won't take ownership.",
                                my_ns, output.metadata.name
                            )));
                        }
                    } else {
                        let own = if output.teardown.unwrap_or(true)
                            && my_ns == self.namespace().unwrap_or(my_ns.clone())
                        {
                            Some(self)
                        } else {
                            None
                        };
                        if let Some(sec) = create!(
                            secrets,
                            "Secret",
                            own,
                            output.clone(),
                            my_ns,
                            &my_values,
                            conditions,
                            recorder,
                            &obj_ref
                        ) && output.teardown.unwrap_or(true)
                        {
                            owned_new.push(OwnedObjects::secret(
                                sec.metadata.name.unwrap().as_str(),
                                sec.metadata.namespace.unwrap().as_str(),
                                sec.metadata.uid.unwrap().as_str(),
                            ));
                        }
                    }
                } else if output.kind == Kind::ConfigMap {
                    let mut cms = ConfigMapHandler::new(&ctx.client.clone(), &my_ns);
                    if self.owned().clone().into_iter().any(|o| {
                        o.kind == output.kind && o.name == output.metadata.name && o.namespace == my_ns
                    }) {
                        let myself = self
                            .owned()
                            .clone()
                            .into_iter()
                            .find(|o| {
                                o.kind == output.kind
                                    && o.name == output.metadata.name
                                    && o.namespace == my_ns
                            })
                            .unwrap();
                        if cms.have_uid(&myself.name, &myself.uid).await {
                            if !cms.have_with_data(&myself.name, &my_values).await {
                                let _ = update!(
                                    cms,
                                    "ConfigMap",
                                    output,
                                    my_ns,
                                    &my_values,
                                    conditions,
                                    recorder,
                                    &obj_ref
                                );
                            }
                            owned_new.push(myself);
                        } else if cms.have(&myself.name).await {
                            if !cms.have_with_data(&myself.name, &my_values).await {
                                let _ = update!(
                                    cms,
                                    "ConfigMap",
                                    output,
                                    my_ns,
                                    &my_values,
                                    conditions,
                                    recorder,
                                    &obj_ref
                                );
                            }
                            if output.teardown.unwrap_or(true) {
                                conditions.push(ApplicationCondition::output_exist(&format!(
                                    "ConfigMap output '{}.{}' already exist, won't take ownership.",
                                    my_ns, output.metadata.name
                                )));
                            }
                        } else {
                            let own = if output.teardown.unwrap_or(true)
                                && my_ns == self.namespace().unwrap_or(my_ns.clone())
                            {
                                Some(self)
                            } else {
                                None
                            };
                            if let Some(sec) = create!(
                                cms,
                                "ConfigMap",
                                own,
                                output,
                                my_ns,
                                &my_values,
                                conditions,
                                recorder,
                                &obj_ref
                            ) && output.teardown.unwrap_or(true)
                            {
                                owned_new.push(OwnedObjects::configmap(
                                    sec.metadata.name.unwrap().as_str(),
                                    sec.metadata.namespace.unwrap().as_str(),
                                    sec.metadata.uid.unwrap().as_str(),
                                ));
                            }
                        }
                    } else if cms.have(&output.metadata.name).await {
                        if !cms.have_with_data(&output.metadata.name, &my_values).await {
                            let _ = update!(
                                cms,
                                "ConfigMap",
                                output,
                                my_ns,
                                &my_values,
                                conditions,
                                recorder,
                                &obj_ref
                            );
                        }
                        if output.teardown.unwrap_or(true) {
                            conditions.push(ApplicationCondition::output_exist(&format!(
                                "ConfigMap output '{}.{}' already exist, won't take ownership.",
                                my_ns, output.metadata.name
                            )));
                        }
                    } else {
                        let own = if output.teardown.unwrap_or(true)
                            && my_ns == self.namespace().unwrap_or(my_ns.clone())
                        {
                            Some(self)
                        } else {
                            None
                        };
                        if let Some(sec) = create!(
                            cms,
                            "ConfigMap",
                            own,
                            output.clone(),
                            my_ns,
                            &my_values,
                            conditions,
                            recorder,
                            &obj_ref
                        ) && output.teardown.unwrap_or(true)
                        {
                            owned_new.push(OwnedObjects::configmap(
                                sec.metadata.name.unwrap().as_str(),
                                sec.metadata.namespace.unwrap().as_str(),
                                sec.metadata.uid.unwrap().as_str(),
                            ));
                        }
                    }
                }
            }
        }
        // Handle every owned object that is not in the new list
        for obj in self.clone().owned() {
            if !owned_new
                .clone()
                .into_iter()
                .any(|n| n.kind == obj.kind && n.namespace == obj.namespace && n.name == obj.name)
            {
                if obj.kind == Kind::Secret {
                    let mut secrets = SecretHandler::new(&ctx.client.clone(), &obj.namespace);
                    if secrets.have_uid(&obj.name, &obj.uid).await {
                        secrets.delete(obj.name.as_str()).await.unwrap_or_else(|e| {
                            conditions.push(ApplicationCondition::output_delete_failed(&format!(
                                "Failed deleting: {}.{}: {:?}",
                                obj.namespace, obj.name, e
                            )));
                            owned_new.push(obj.clone());
                        })
                    }
                } else if obj.kind == Kind::ConfigMap {
                    let mut cms = ConfigMapHandler::new(&ctx.client.clone(), &obj.namespace);
                    if cms.have_uid(&obj.name, &obj.uid).await {
                        cms.delete(obj.name.as_str()).await.unwrap_or_else(|e| {
                            conditions.push(ApplicationCondition::output_delete_failed(&format!(
                                "Failed deleting: {}.{}: {:?}",
                                obj.namespace, obj.name, e
                            )));
                            owned_new.push(obj.clone());
                        })
                    }
                }
            }
        }

        // Verify that everything went fine and update the status accordingly
        if conditions.iter().any(|c| {
            c.condition_type != ConditionsType::InputMissing
                && c.condition_type != ConditionsType::ReadMissing
                && c.condition_type != ConditionsType::WriteAlreadyExist
                && c.condition_type != ConditionsType::OutputAlreadyExist
        }) {
            let msg = "Some output have failed";
            Self::publish_warning(
                &recorder,
                &obj_ref,
                String::from(msg),
                format!("Found {} error(s) while creating outputs", conditions.len()),
                String::from("fail"),
            )
            .await?;
            conditions.push(ApplicationCondition::not_ready(msg));
            let new_status = Patch::Apply(json!({
                "apiVersion": "kuberest.solidite.fr/v1",
                "kind": "RestEndPoint",
                "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: owned_new, owned_target: target_new }
            }));
            let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
            let _o = restendpoints
                .patch_status(&name, &ps, &new_status)
                .await
                .map_err(Error::KubeError)?;
            Ok(Action::requeue(Duration::from_secs(
                self.spec.retry_frequency.unwrap_or(5 * 60),
            )))
        } else {
            let msg = "All done";
            conditions.push(ApplicationCondition::is_ready(msg));
            let new_status = Patch::Apply(json!({
                "apiVersion": "kuberest.solidite.fr/v1",
                "kind": "RestEndPoint",
                "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: owned_new, owned_target: target_new }
            }));
            let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
            let _o = restendpoints
                .patch_status(&name, &ps, &new_status)
                .await
                .map_err(Error::KubeError)?;
            let wait_duration = self.spec.check_frequency.unwrap_or(60 * 60);
            if wait_duration > 0 {
                Ok(Action::requeue(Duration::from_secs(wait_duration)))
            } else {
                Ok(Action::await_change())
            }
        }
    }

    // Finalizer cleanup (the object was deleted, ensure nothing is orphaned)
    async fn cleanup(&self, ctx: Arc<Context>) -> Result<Action> {
        let client = ctx.client.clone();
        ctx.diagnostics.write().await.last_event = Utc::now();
        let reporter = ctx.diagnostics.read().await.reporter.clone();
        let recorder = Recorder::new(client.clone(), reporter);
        let obj_ref = self.object_ref(&());
        let ns = self.namespace().unwrap();
        let name = self.name_any();
        ctx.wakeups.remove(&ns, &name);
        let restendpoints: Api<RestEndPoint> = Api::namespaced(client.clone(), &ns);
        let mut conditions: Vec<ApplicationCondition> = Vec::new();
        let mut values = serde_json::json!({"input":{},"pre":{}});
        let mut hbs = handlebarshandler::new_hbs();
        if let Some(templates) = self.spec.templates.clone() {
            for item in templates {
                hbs.register_template(&item.name, &item.template)
                    .unwrap_or_else(|e| {
                        conditions.push(ApplicationCondition::template_failed(&format!(
                            "Registering template.{} raised {e:?}",
                            item.name
                        )));
                    });
            }
        }
        let use_multi = std::env::var("MULTI_TENANT")
            .unwrap_or_else(|_| "true".to_string())
            .to_lowercase()
            == *"true";
        let allowed = if use_multi && (self.spec.inputs.is_some() || self.spec.outputs.is_some()) {
            self.get_tenant_namespaces(client.clone()).await?
        } else {
            Vec::new()
        };
        let mut do_prepare_client = true;
        if self.status.is_none() {
            // Nothing to do, since there's nothing to delete
            return Ok(Action::await_change());
        } else if self.spec.teardown.is_none() {
            // no teardown script, only prepare client if there are some write to delete
            let status = self.status.clone().unwrap();
            do_prepare_client = !status.owned_target.is_empty();
        }
        if !do_prepare_client {
            tracing::info!("Skipping to prepare the client");
        }
        let mut rhai = Script::new();
        rhai.ctx.set_or_push("hbs", hbs.clone());
        // Read the inputs first
        if do_prepare_client {
            if let Some(inputs) = self.spec.clone().inputs {
                for input in inputs {
                    if let Some(secret) = input.secret_ref {
                        let my_ns = if use_multi {
                            if let Some(o_ns) = secret.namespace.clone() {
                                if allowed.contains(&o_ns) {
                                    o_ns
                                } else {
                                    Self::publish_warning(&recorder, &obj_ref, String::from("IgnoredNamespace"), format!("Ignoring namespace from Input '{}' as the operator run in multi-tenant mode",input.name), String::from("readingInput")).await?;
                                    ns.clone()
                                }
                            } else {
                                ns.clone()
                            }
                        } else if let Some(local_ns) = secret.namespace {
                            local_ns
                        } else {
                            ns.clone()
                        };
                        let mut secrets = SecretHandler::new(&ctx.client.clone(), &my_ns);
                        match secrets.get(&secret.name).await {
                            Ok(my_secret) => {
                                values["input"][input.name] = serde_json::json!({
                                    "metadata": my_secret.metadata,
                                    "data": my_secret.data
                                });
                            }
                            Err(e) => {
                                if secret.optional.unwrap_or(false) {
                                    let _ = Self::publish(
                                        &recorder,
                                        &obj_ref,
                                        String::from("IgnoredInput"),
                                        format!("Ignoring not found secret for Input '{}'", input.name),
                                        String::from("readingInput"),
                                    )
                                    .await;
                                    warn!(
                                        "Optional secret '{}' not found for Input '{}': {}",
                                        secret.name, input.name, e
                                    );
                                    conditions.push(ApplicationCondition::input_missing(&format!(
                                        "Input '{}' Secret {}.{} not found",
                                        input.name, my_ns, secret.name
                                    )));
                                    values["input"][input.name] = serde_json::json!({});
                                } else {
                                    let _ = Self::publish(
                                        &recorder,
                                        &obj_ref,
                                        String::from("MissingSecret"),
                                        format!(
                                            "Secret '{}' not found for Input '{}'",
                                            secret.name, input.name
                                        ),
                                        String::from("readingInput"),
                                    )
                                    .await;
                                    error!(
                                        "Required secret '{}' not found for Input '{}': {}",
                                        secret.name, input.name, e
                                    );
                                    conditions.push(ApplicationCondition::input_failed(&format!(
                                        "Input '{}' Secret {}.{} not found",
                                        input.name, my_ns, secret.name
                                    )));
                                }
                            }
                        }
                    } else if let Some(cfgmap) = input.config_map_ref {
                        let my_ns = if use_multi {
                            if let Some(o_ns) = cfgmap.namespace.clone() {
                                if allowed.contains(&o_ns) {
                                    o_ns
                                } else {
                                    Self::publish_warning(&recorder, &obj_ref, String::from("IgnoredNamespace"), format!("Ignoring namespace from Input '{}' as the operator run in multi-tenant mode",input.name), String::from("readingInput")).await?;
                                    ns.clone()
                                }
                            } else {
                                ns.clone()
                            }
                        } else if let Some(local_ns) = cfgmap.namespace {
                            local_ns
                        } else {
                            ns.clone()
                        };
                        let mut maps = ConfigMapHandler::new(&ctx.client.clone(), &my_ns);
                        match maps.get(&cfgmap.name).await {
                            Ok(my_cfg) => {
                                values["input"][input.name] = serde_json::json!({
                                    "metadata": my_cfg.metadata,
                                    "data": my_cfg.data,
                                    "binaryData": my_cfg.binary_data
                                });
                            }
                            Err(e) => {
                                if cfgmap.optional.unwrap_or(false) {
                                    let _ = Self::publish(
                                        &recorder,
                                        &obj_ref,
                                        String::from("IgnoredInput"),
                                        format!("Ignoring not found ConfigMap for Input '{}'", input.name),
                                        String::from("readingInput"),
                                    )
                                    .await;
                                    warn!(
                                        "Optional configmap '{}' not found for Input '{}': {}",
                                        cfgmap.name, input.name, e
                                    );
                                    conditions.push(ApplicationCondition::input_missing(&format!(
                                        "Input '{}' ConfigMap {}.{} not found",
                                        input.name, my_ns, cfgmap.name
                                    )));
                                    values["input"][input.name] = serde_json::json!({});
                                } else {
                                    let _ = Self::publish(
                                        &recorder,
                                        &obj_ref,
                                        String::from("MissingConfigMap"),
                                        format!(
                                            "ConfigMap '{}' not found for Input '{}'",
                                            cfgmap.name, input.name
                                        ),
                                        String::from("readingInput"),
                                    )
                                    .await;
                                    error!(
                                        "Required configmap '{}' not found for Input '{}': {}",
                                        cfgmap.name, input.name, e
                                    );
                                    conditions.push(ApplicationCondition::input_failed(&format!(
                                        "Input '{}' ConfigMap {}.{} not found",
                                        input.name, my_ns, cfgmap.name
                                    )));
                                }
                            }
                        }
                    } else if let Some(render) = input.handle_bars_render {
                        values["input"][input.name] = json!(template!(
                            render.as_str(),
                            hbs,
                            &values,
                            conditions,
                            recorder,
                            &obj_ref
                        ));
                    } else if let Some(password_def) = input.password_generator {
                        values["input"][input.name] = json!(Passwords::new().generate(
                            password_def.length.unwrap_or(32),
                            password_def.weight_alphas.unwrap_or(60),
                            password_def.weight_numbers.unwrap_or(20),
                            password_def.weight_symbols.unwrap_or(20)
                        ));
                    } else {
                        Self::publish_warning(
                            &recorder,
                            &obj_ref,
                            String::from("EmptyInput"),
                            format!("Input '{}' have no source", input.name),
                            String::from("fail"),
                        )
                        .await?;
                        conditions.push(ApplicationCondition::input_failed(&format!(
                            "Input '{}' have no source",
                            input.name
                        )));
                        conditions.push(ApplicationCondition::not_ready(&format!(
                            "Input '{}' have no source",
                            input.name
                        )));
                        let new_status = Patch::Apply(json!({
                            "apiVersion": "kuberest.solidite.fr/v1",
                            "kind": "RestEndPoint",
                            "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: self.owned_target() }
                        }));
                        let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
                        let _o = restendpoints
                            .patch_status(&name, &ps, &new_status)
                            .await
                            .map_err(Error::KubeError)?;
                        return Err(Error::IllegalRestEndPoint);
                    }
                }
            }
            rhai.set_dynamic("input", &values["input"]);
            rhai.set_dynamic("values", &values);

            // Run the init script
            if let Some(script) = self.spec.init.clone() {
                let cnd = conditions.clone();
                values["init"] = rhai.eval(&script).unwrap_or_else(|e| {
                    conditions.push(ApplicationCondition::init_script_failed(&format!("{e:?}")));
                    json!({})
                });
                // Validate that init-script went Ok
                if cnd
                    .iter()
                    .any(|c| c.condition_type != ConditionsType::InputMissing)
                {
                    let msg = "Init-script failed";
                    Self::publish_warning(
                        &recorder,
                        &obj_ref,
                        String::from(msg),
                        format!("Found {} error(s) running the init-script", conditions.len()),
                        String::from("fail"),
                    )
                    .await?;
                    tokio::time::sleep(TEARDOWN_BACKOFF).await;
                    conditions.push(ApplicationCondition::not_ready(msg));
                    let new_status = Patch::Apply(json!({
                        "apiVersion": "kuberest.solidite.fr/v1",
                        "kind": "RestEndPoint",
                        "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: self.owned_target() }
                    }));
                    let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
                    let _o = restendpoints
                        .patch_status(&name, &ps, &new_status)
                        .await
                        .map_err(Error::KubeError)?;
                    // An Ok would remove the finalizer and orphan what is still to delete
                    return Err(Error::TeardownIncomplete);
                }
            }
            rhai.set_dynamic("init", &values["init"]);
            rhai.set_dynamic("values", &values);
        }

        // Setup the httpClient
        // The base url may need the inputs, which are only read when there is something to call
        let mut rest = RestClient::new("");
        if do_prepare_client {
            rest = RestClient::new(&template!(
                self.spec.client.baseurl.clone().as_str(),
                hbs,
                &values,
                conditions,
                recorder,
                &obj_ref
            ));
            if let Some(headers) = self.spec.client.headers.clone() {
                for (key, value) in headers {
                    rest.add_header(
                        &template!(key.as_str(), hbs, &values, conditions, recorder, &obj_ref),
                        &template!(value.as_str(), hbs, &values, conditions, recorder, &obj_ref),
                    );
                }
            }
            rest.add_header_json();
            if let Some(ca) = self.spec.client.server_ca.clone() {
                rest.set_server_ca(&template!(
                    &ca.as_str(),
                    hbs,
                    &values,
                    conditions,
                    recorder,
                    &obj_ref
                ));
            }
            if self.spec.client.client_cert.is_some() && self.spec.client.client_key.is_some() {
                rest.set_mtls(
                    &template!(
                        &self.spec.client.client_cert.clone().unwrap().as_str(),
                        hbs,
                        &values,
                        conditions,
                        recorder,
                        &obj_ref
                    ),
                    &template!(
                        &self.spec.client.client_key.clone().unwrap().as_str(),
                        hbs,
                        &values,
                        conditions,
                        recorder,
                        &obj_ref
                    ),
                );
            }
            rhai.ctx.set_or_push("client", rest.clone());
        }

        let mut owned_new: Vec<OwnedObjects> = Vec::new();
        let mut target_new: Vec<OwnedRestPoint> = Vec::new();

        // Start the teardown script
        if let Some(script) = self.spec.teardown.clone() {
            let cnd = conditions.clone();
            values["pre"] = rhai.eval(&script).unwrap_or_else(|e| {
                conditions.push(ApplicationCondition::post_script_failed(&format!("{e:?}")));
                json!({})
            });
            values["teardown"] = values["pre"].clone();
            // Validate that teardown went Ok
            if cnd
                .iter()
                .any(|c| c.condition_type != ConditionsType::InputMissing)
            {
                let msg = "Teardown failed";
                Self::publish_warning(
                    &recorder,
                    &obj_ref,
                    String::from(msg),
                    format!("Found {} error(s) during teardown", conditions.len()),
                    String::from("fail"),
                )
                .await?;
                tokio::time::sleep(TEARDOWN_BACKOFF).await;
                conditions.push(ApplicationCondition::not_ready(msg));
                let new_status = Patch::Apply(json!({
                    "apiVersion": "kuberest.solidite.fr/v1",
                    "kind": "RestEndPoint",
                    "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: self.owned(), owned_target: self.owned_target() }
                }));
                let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
                let _o = restendpoints
                    .patch_status(&name, &ps, &new_status)
                    .await
                    .map_err(Error::KubeError)?;
                // An Ok would remove the finalizer and orphan what is still to delete
                return Err(Error::TeardownIncomplete);
            }
        }

        // Delete writes
        if let Some(status) = self.status.clone() {
            for obj in status.owned_target {
                if obj.teardown && !obj.key.is_empty() {
                    let delete_result = if obj.children_no_sub_key.unwrap_or(false) {
                        let del_key_name = obj.key_name.clone().unwrap_or("id".to_string());
                        rest.obj_delete_with_body(
                            DeleteMethod::Delete,
                            &obj.path,
                            &json!({del_key_name: obj.key}),
                        )
                    } else {
                        {
                            let (dp, dk) = obj.delete_target();
                            rest.obj_delete(DeleteMethod::Delete, &dp, &dk)
                        }
                    };
                    delete_result.unwrap_or_else(|e| {
                        let giveup = if let vynil_core::Error::MethodFailed(_, code, _) = e {
                            code == 404
                        } else {
                            false
                        };
                        // Allow the user to quit the finalizer loop by setting spec.client.teardown to true
                        if !self.spec.client.teardown.unwrap_or(false) && !giveup {
                            target_new.push(obj.clone());
                            conditions.push(ApplicationCondition::write_delete_failed(
                                format!(
                                    "Deleting write.{}.{} {e:?}",
                                    obj.group.as_str(),
                                    obj.name.as_str()
                                )
                                .as_str(),
                            ));
                        }
                        tokio::task::block_in_place(|| {
                            Handle::current().block_on(async {
                                Self::publish_warning(
                                    &recorder,
                                    &obj_ref,
                                    format!("Failed deleting: write.{}.{}", obj.group, obj.name),
                                    format!("{e:?}"),
                                    String::from("deleting"),
                                )
                                .await
                                .unwrap_or(());
                            })
                        });
                        json!({})
                    });
                }
            }
        }

        // Delete owned objects
        if let Some(status) = self.status.clone() {
            for obj in status.owned {
                if obj.kind == Kind::Secret {
                    let mut secrets = SecretHandler::new(&ctx.client.clone(), &obj.namespace);
                    if secrets.have_uid(&obj.name, &obj.uid).await {
                        secrets.delete(obj.name.as_str()).await.unwrap_or_else(|e| {
                            conditions.push(ApplicationCondition::output_delete_failed(&format!(
                                "Failed deleting: {}.{}: {:?}",
                                obj.namespace, obj.name, e
                            )));
                            owned_new.push(obj.clone());
                        })
                    }
                } else if obj.kind == Kind::ConfigMap {
                    let mut secrets = ConfigMapHandler::new(&ctx.client.clone(), &obj.namespace);
                    if secrets.have_uid(&obj.name, &obj.uid).await {
                        secrets.delete(obj.name.as_str()).await.unwrap_or_else(|e| {
                            conditions.push(ApplicationCondition::output_delete_failed(&format!(
                                "Failed deleting: {}.{}: {:?}",
                                obj.namespace, obj.name, e
                            )));
                            owned_new.push(obj.clone());
                        })
                    }
                }
            }
        }
        if conditions.iter().any(|c| {
            c.condition_type != ConditionsType::InputMissing
                && c.condition_type != ConditionsType::ReadMissing
                && c.condition_type != ConditionsType::WriteAlreadyExist
                && c.condition_type != ConditionsType::OutputAlreadyExist
        }) || !owned_new.is_empty()
            || !target_new.is_empty()
        {
            let msg = "Some teardown failed";
            Self::publish_warning(
                &recorder,
                &obj_ref,
                String::from(msg),
                format!("Found {} error(s) during teardown", conditions.len()),
                String::from("fail"),
            )
            .await?;
            // Wait before reporting the failure, because the controller keeps trying and it might hammer the api-server and the api-endpoint targeted otherwise. Beside the events generated already informed the user of the issue
            tokio::time::sleep(TEARDOWN_BACKOFF).await;
            conditions.push(ApplicationCondition::not_ready(msg));
            let new_status = Patch::Apply(json!({
                "apiVersion": "kuberest.solidite.fr/v1",
                "kind": "RestEndPoint",
                "status": RestEndPointStatus { conditions, generation: self.metadata.generation.unwrap_or(1), owned: owned_new, owned_target: target_new }
            }));
            let ps = PatchParams::apply(RESTPATH_FINALIZER).force();
            let _o = restendpoints
                .patch_status(&name, &ps, &new_status)
                .await
                .map_err(Error::KubeError)?;
            Err(Error::TeardownIncomplete)
        } else {
            Ok(Action::await_change())
        }
    }
}

/// Diagnostics to be exposed by the web server
#[derive(Clone, Serialize)]
pub struct Diagnostics {
    #[serde(deserialize_with = "from_ts")]
    pub last_event: DateTime<Utc>,
    #[serde(skip)]
    pub reporter: Reporter,
}
impl Default for Diagnostics {
    fn default() -> Self {
        Self {
            last_event: Utc::now(),
            reporter: "restendpoint-controller".into(),
        }
    }
}
impl Diagnostics {
    fn recorder(&self, client: Client) -> Recorder {
        Recorder::new(client, self.reporter.clone())
    }
}

/// State shared between the controller and the web server
#[derive(Clone, Default)]
pub struct State {
    /// Diagnostics populated by the reconciler
    diagnostics: Arc<RwLock<Diagnostics>>,
    /// Metrics registry
    registry: prometheus::Registry,
}

/// State wrapper around the controller outputs for the web server
impl State {
    /// Metrics getter
    pub fn metrics(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }

    /// State getter
    pub async fn diagnostics(&self) -> Diagnostics {
        self.diagnostics.read().await.clone()
    }

    // Create a Controller Context that can update State
    pub fn to_context(&self, client: Client) -> Arc<Context> {
        Arc::new(Context {
            client,
            metrics: Metrics::default().register(&self.registry).unwrap(),
            diagnostics: self.diagnostics.clone(),
            wakeups: Wakeups::default(),
        })
    }
}

/// Initialize the controller and shared state (given the crd is installed)
pub async fn run(state: State) {
    let client: Client = Client::try_default().await.expect("failed to create kube Client");
    let restendpoints = Api::<RestEndPoint>::all(client.clone());
    if let Err(e) = restendpoints.list(&ListParams::default().limit(1)).await {
        error!("CRD is not queryable; {e:?}. Is the CRD installed?");
        info!("Installation: cargo run --bin crdgen | kubectl apply -f -");
        std::process::exit(1);
    }
    let ctx = state.to_context(client.clone());
    let controller = Controller::new(restendpoints, Config::default().any_semantic());
    let store = controller.store();
    // Metadata-only watches: the content of the Secrets never enters the memory of the watcher
    let secrets = metadata_watcher(Api::<Secret>::all(client.clone()), Config::default()).touched_objects();
    let configmaps =
        metadata_watcher(Api::<ConfigMap>::all(client.clone()), Config::default()).touched_objects();
    let (st, wk) = (store.clone(), ctx.wakeups.clone());
    let on_secret =
        move |o: PartialObjectMeta<Secret>| input_changed(InputKind::Secret, &o.metadata, &st, &wk);
    let (st, wk) = (store.clone(), ctx.wakeups.clone());
    let on_configmap =
        move |o: PartialObjectMeta<ConfigMap>| input_changed(InputKind::ConfigMap, &o.metadata, &st, &wk);
    // Events mapped before the store is filled wake nobody up: the first reconcile of each RestEndPoint
    // covers them. Don't hold these streams on `Store::wait_until_ready()`: in kube-runtime 3.1.0 it only
    // wakes up its last waiter, and the applier already waits on it.
    controller
        .watches_stream(secrets, on_secret)
        .watches_stream(configmaps, on_configmap)
        .shutdown_on_signal()
        .run(reconcile, error_policy, ctx)
        .filter_map(|x| async move { std::result::Result::ok(x) })
        .for_each(|_| futures::future::ready(()))
        .await;
}

/// Map a changed Secret/ConfigMap to the RestEndPoints to wake up
fn input_changed(
    kind: InputKind,
    meta: &kube::api::ObjectMeta,
    store: &Store<RestEndPoint>,
    wakeups: &Wakeups,
) -> Vec<ObjectRef<RestEndPoint>> {
    // Every Secret and ConfigMap of the cluster goes through here: don't copy the store for nothing
    if store.find(|r| r.spec.watch_inputs == Some(true)).is_none() || written_by_us(meta) {
        return Vec::new();
    }
    let (ns, name) = (
        meta.namespace.clone().unwrap_or_default(),
        meta.name.clone().unwrap_or_default(),
    );
    endpoints_using(kind, &ns, &name, &store.state())
        .into_iter()
        .map(|(rep_ns, rep_name)| {
            wakeups.insert(&rep_ns, &rep_name);
            ObjectRef::new(&rep_name).within(&rep_ns)
        })
        .collect()
}

// Mock tests relying on fixtures.rs and its primitive apiserver mocks
#[cfg(test)]
mod test {
    use super::{
        ApplicationCondition, ConditionsStatus, ConditionsType, Context, OwnedRestPoint, RestEndPoint,
        RestEndPointStatus, /* error_policy, */ reconcile,
    };
    use crate::fixtures::{Scenario, timeout_after_1s};
    use kube::runtime::controller::Action;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::time::Duration;

    #[tokio::test]
    async fn restendpoint_without_finalizer_gets_a_finalizer() {
        let (testctx, fakeserver, _) = Context::test();
        let restendpoint = RestEndPoint::test();
        let mocksrv = fakeserver.run(Scenario::FinalizerCreation(restendpoint.clone()));
        reconcile(Arc::new(restendpoint), testctx)
            .await
            .expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }

    type Calls = Arc<std::sync::Mutex<Vec<(http::Method, String, serde_json::Value)>>>;

    /// An apiserver without any Secret nor ConfigMap, which records the calls it receives
    fn apiserver_without_inputs(rep: RestEndPoint) -> (Arc<Context>, Calls) {
        use http::{Request, Response};
        use kube::client::Body;
        let (service, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
        let calls = Calls::default();
        let record = calls.clone();
        tokio::spawn(async move {
            while let Some((request, send)) = handle.next_request().await {
                let (method, path) = (request.method().clone(), request.uri().path().to_string());
                let bytes = request.into_body().collect_bytes().await.unwrap();
                let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
                record.lock().unwrap().push((method, path.clone(), body.clone()));
                let (code, answer) = if path.contains("/secrets/") || path.contains("/configmaps/") {
                    (
                        404,
                        json!({"kind":"Status","apiVersion":"v1","status":"Failure","message":"not found","reason":"NotFound","code":404}),
                    )
                } else if path.contains("/events") {
                    (200, body)
                } else if path.starts_with("/apis/kuberest.solidite.fr/") {
                    (200, serde_json::to_value(&rep).unwrap())
                } else {
                    (
                        200,
                        json!({"apiVersion":"meta.k8s.io/v1","kind":"PartialObjectMetadata","metadata":{"name":"default"}}),
                    )
                };
                let answer = Body::from(serde_json::to_vec(&answer).unwrap());
                send.send_response(Response::builder().status(code).body(answer).unwrap());
            }
        });
        let ctx = Context {
            client: kube::Client::new(service, "default"),
            metrics: Default::default(),
            diagnostics: Arc::default(),
            wakeups: Default::default(),
        };
        (Arc::new(ctx), calls)
    }

    /// The status sent by the last status patch
    fn patched_status(calls: &Calls) -> RestEndPointStatus {
        let calls = calls.lock().unwrap();
        let (_, _, body) = calls
            .iter()
            .rfind(|(_, path, _)| path.ends_with("/status"))
            .expect("a status patch");
        serde_json::from_value(body["status"].clone()).expect("valid status")
    }

    fn finalizer_was_removed(calls: &Calls) -> bool {
        calls
            .lock()
            .unwrap()
            .iter()
            .any(|(method, path, _)| *method == http::Method::PATCH && path.ends_with("/restendpoints/test"))
    }

    fn assert_input_failed(status: &RestEndPointStatus, message: &str) {
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.condition_type == ConditionsType::InputFailed && c.message == message),
            "{:?}",
            status.conditions
        );
        assert!(
            status.conditions.iter().any(|c| {
                c.condition_type == ConditionsType::Ready && c.status == ConditionsStatus::False
            })
        );
    }

    /// A finalized RestEndPoint with a required input which does not exist
    fn rep_missing(secret: bool, retry_frequency: Option<u64>) -> RestEndPoint {
        let mut rep = (*rep_with(
            "test",
            "default",
            false,
            secret.then_some(("later", None)),
            (!secret).then_some("later"),
        ))
        .clone()
        .finalized();
        rep.spec.retry_frequency = retry_frequency;
        rep
    }

    fn an_owned_write() -> OwnedRestPoint {
        OwnedRestPoint::new("things", "k1", "group", "item", true, false, "id")
    }

    fn status_with(owned_target: Vec<OwnedRestPoint>) -> RestEndPointStatus {
        RestEndPointStatus {
            conditions: vec![ApplicationCondition::is_ready("done")],
            generation: 1,
            owned: Vec::new(),
            owned_target,
        }
    }

    #[tokio::test]
    async fn a_missing_secret_is_reported_and_retried_after_one_minute() {
        let rep = rep_missing(true, None);
        let (ctx, calls) = apiserver_without_inputs(rep.clone());
        let action = reconcile(Arc::new(rep.clone()), ctx.clone())
            .await
            .expect("reported");
        assert_eq!(action, Action::requeue(Duration::from_secs(60)));
        let status = patched_status(&calls);
        assert_input_failed(&status, "Input 'i' Secret default.later not found");
        // the reconcile triggered by our own status patch must not postpone this first retry
        let action = reconcile(Arc::new(rep.with_status(status)), ctx.clone())
            .await
            .expect("guarded");
        let wait = ctx.wakeups.due_in("default", "test").expect("a wake-up");
        assert!(wait <= Duration::from_secs(60), "{wait:?}");
        assert_ne!(action, Action::requeue(Duration::from_secs(300)));
    }

    #[tokio::test]
    async fn a_short_retry_frequency_also_applies_to_the_first_retry() {
        let rep = rep_missing(true, Some(30));
        let (ctx, _) = apiserver_without_inputs(rep.clone());
        let action = reconcile(Arc::new(rep), ctx).await.expect("reported");
        assert_eq!(action, Action::requeue(Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn a_missing_configmap_replaces_a_stale_ready_status() {
        let mut rep = rep_missing(false, Some(120)).with_status(status_with(vec![an_owned_write()]));
        rep.metadata.generation = Some(2);
        let (ctx, calls) = apiserver_without_inputs(rep.clone());
        let action = reconcile(Arc::new(rep), ctx.clone()).await.expect("reported");
        assert_eq!(action, Action::requeue(Duration::from_secs(120)));
        let status = patched_status(&calls);
        assert_input_failed(&status, "Input 'i' ConfigMap default.later not found");
        assert_eq!(status.owned_target, vec![an_owned_write()]);
        assert!(ctx.wakeups.due_in("default", "test").is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn teardown_goes_on_with_a_missing_input_and_reports_it() {
        let mut rep = rep_missing(true, None)
            .with_status(status_with(vec![an_owned_write()]))
            .needs_delete();
        rep.spec.client.baseurl = "http://127.0.0.1:1".into();
        // the test default is `true`, which ignores the delete failures
        rep.spec.client.teardown = None;
        let (ctx, calls) = apiserver_without_inputs(rep.clone());
        // required before any REST call (done by main in the controller)
        vynil_core::set_client_name(super::get_client_name);
        assert!(reconcile(Arc::new(rep), ctx).await.is_err());
        let status = patched_status(&calls);
        assert_input_failed(&status, "Input 'i' Secret default.later not found");
        // the delete was attempted and failed: the object is still ours
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.condition_type == ConditionsType::WriteDeleteFailed)
        );
        assert_eq!(status.owned_target, vec![an_owned_write()]);
        assert!(!finalizer_was_removed(&calls));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_init_or_teardown_script_keeps_the_finalizer() {
        for init in [true, false] {
            let mut rep = rep_missing(true, None)
                .with_status(status_with(vec![an_owned_write()]))
                .needs_delete();
            if init {
                rep.spec.init = Some("#{}".into());
            } else {
                rep.spec.teardown = Some("#{}".into());
            }
            let (ctx, calls) = apiserver_without_inputs(rep.clone());
            assert!(reconcile(Arc::new(rep), ctx).await.is_err(), "init: {init}");
            assert_eq!(patched_status(&calls).owned_target, vec![an_owned_write()]);
            assert!(!finalizer_was_removed(&calls), "init: {init}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn teardown_ends_once_nothing_is_left_to_delete() {
        // the second pass after `teardown_goes_on_...` : the inputs are not read anymore
        let mut rep = rep_missing(true, None)
            .with_status(status_with(Vec::new()))
            .needs_delete();
        rep.spec.client.baseurl = "http://{{base64_decode input.i.data.host}}".into();
        let (ctx, calls) = apiserver_without_inputs(rep.clone());
        reconcile(Arc::new(rep), ctx).await.expect("released");
        assert!(finalizer_was_removed(&calls));
    }

    #[tokio::test]
    async fn finalized_restendpoint_causes_status_patch() {
        let (testctx, fakeserver, _) = Context::test();
        let restendpoint = RestEndPoint::test().finalized();
        let mocksrv = fakeserver.run(Scenario::StatusPatch(restendpoint.clone()));
        reconcile(Arc::new(restendpoint), testctx)
            .await
            .expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }

    /* TODO: include more Unit-tests
        #[tokio::test]
        async fn finalized_restendpoint_with_hide_causes_event_and_hide_patch() {
            let (testctx, fakeserver, _) = Context::test();
            let restendpoint = RestEndPoint::test().finalized().needs_hide();
            let scenario = Scenario::EventPublishThenStatusPatch("HideRequested".into(), restendpoint.clone());
            let mocksrv = fakeserver.run(scenario);
            reconcile(Arc::new(restendpoint), testctx).await.expect("reconciler");
            timeout_after_1s(mocksrv).await;
        }
    #[tokio::test]
    async fn finalized_restendpoint_with_delete_timestamp_causes_delete() {
        let (testctx, fakeserver, _) = Context::test();
        let restendpoint = RestEndPoint::test().finalized().needs_delete();
        let mocksrv = fakeserver.run(Scenario::Cleanup("DeleteRequested".into(), restendpoint.clone()));
        reconcile(Arc::new(restendpoint), testctx)
            .await
            .expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }
        #[tokio::test]
        async fn illegal_restendpoint_reconcile_errors_which_bumps_failure_metric() {
            let (testctx, fakeserver, _registry) = Context::test();
            let restendpoint = Arc::new(RestEndPoint::illegal().finalized());
            let mocksrv = fakeserver.run(Scenario::RadioSilence);
            let res = reconcile(restendpoint.clone(), testctx.clone()).await;
            timeout_after_1s(mocksrv).await;
            assert!(res.is_err(), "apply reconciler fails on illegal restendpoint");
            let err = res.unwrap_err();
            assert!(err.to_string().contains("IllegalRestEndPoint"));
            // calling error policy with the reconciler error should cause the correct metric to be set
            error_policy(restendpoint.clone(), &err, testctx.clone());
            //dbg!("actual metrics: {}", registry.gather());
            let failures = testctx
                .metrics
                .failures
                .with_label_values(&["illegal", "finalizererror(applyfailed(illegalrestendpointument))"])
                .get();
            assert_eq!(failures, 1);
        }
    */
    /*
        // Integration test without mocks
        use kube::api::{Api, ListParams, Patch, PatchParams};
        #[tokio::test]
        #[ignore = "uses k8s current-context"]
        async fn integration_reconcile_should_set_status_and_send_event() {
            let client = kube::Client::try_default().await.unwrap();
            let ctx = super::State::default().to_context(client.clone());

            // create a test restendpoint
            let restendpoint = RestEndPoint::test().finalized().needs_hide();
            let restendpoints: Api<RestEndPoint> = Api::namespaced(client.clone(), "default");
            let ssapply = PatchParams::apply("ctrltest");
            let patch = Patch::Apply(restendpoint.clone());
            restendpoints.patch("test", &ssapply, &patch).await.unwrap();

            // reconcile it (as if it was just applied to the cluster like this)
            reconcile(Arc::new(restendpoint), ctx).await.unwrap();

            // verify side-effects happened
            let output = restendpoints.get_status("test").await.unwrap();
            assert!(output.status.is_some());
            // verify hide event was found
            let events: Api<k8s_openapi::api::core::v1::Event> = Api::all(client.clone());
            let opts = ListParams::default().fields("involvedObject.kind=RestEndPoint,involvedObject.name=test");
            let event = events
                .list(&opts)
                .await
                .unwrap()
                .into_iter()
                .filter(|e| e.reason.as_deref() == Some("HideRequested"))
                .last()
                .unwrap();
            dbg!("got ev: {:?}", &event);
            assert_eq!(event.action.as_deref(), Some("Hiding"));
        }
    */

    fn rep_with(
        name: &str,
        ns: &str,
        watch: bool,
        secret: Option<(&str, Option<&str>)>,
        cm: Option<&str>,
    ) -> Arc<RestEndPoint> {
        use super::{ConfigMapRef, InputItem, SecretRef};
        use kube::Resource;
        let mut r = RestEndPoint::test();
        r.meta_mut().name = Some(name.into());
        r.meta_mut().namespace = Some(ns.into());
        r.spec.watch_inputs = Some(watch);
        r.spec.inputs = Some(vec![InputItem {
            name: "i".into(),
            secret_ref: secret.map(|(n, ns)| SecretRef {
                name: n.into(),
                namespace: ns.map(Into::into),
                optional: None,
            }),
            config_map_ref: cm.map(|n| ConfigMapRef {
                name: n.into(),
                namespace: None,
                optional: None,
            }),
            handle_bars_render: None,
            password_generator: None,
        }]);
        Arc::new(r)
    }

    #[test]
    fn endpoints_using_maps_inputs_to_endpoints_with_watch_inputs() {
        use super::{InputKind, endpoints_using};
        let eps = vec![
            rep_with("a", "ns1", true, Some(("sec", None)), None),
            rep_with("b", "ns1", false, Some(("sec", None)), None), // watchInputs off
            rep_with("c", "ns2", true, Some(("sec", None)), None),  // same name, other namespace
            rep_with("d", "ns2", true, Some(("sec", Some("ns1"))), None), // cross-namespace ref
            rep_with("e", "ns1", true, None, Some("sec")),          // a ConfigMap, not a Secret
        ];
        let mut got = endpoints_using(InputKind::Secret, "ns1", "sec", &eps);
        got.sort();
        assert_eq!(got, vec![
            ("ns1".to_string(), "a".to_string()),
            ("ns2".to_string(), "d".to_string())
        ]);
        assert_eq!(endpoints_using(InputKind::ConfigMap, "ns1", "sec", &eps), vec![(
            "ns1".to_string(),
            "e".to_string()
        )]);
        assert!(endpoints_using(InputKind::Secret, "ns1", "other", &eps).is_empty());
    }

    #[test]
    fn wakeups_are_consumed_once() {
        let w = super::Wakeups::default();
        assert!(!w.take_due("ns", "a"));
        w.insert("ns", "a");
        w.insert("ns", "a");
        assert!(w.take_due("ns", "a"));
        assert!(!w.take_due("ns", "a"));
    }

    #[test]
    fn a_retried_wakeup_waits_for_its_delay() {
        let w = super::Wakeups::default();
        w.retry_after("ns", "a", Duration::from_secs(300));
        assert!(!w.take_due("ns", "a"), "not due yet");
        assert!(w.due_in("ns", "a").is_some_and(|d| d > Duration::from_secs(200)));
        // a new change of an input does not wait for the retry
        w.insert("ns", "a");
        assert!(w.take_due("ns", "a"));
        // and a retry does not delay a change which is already recorded
        w.insert("ns", "a");
        w.retry_after("ns", "a", Duration::from_secs(300));
        assert!(w.take_due("ns", "a"));
        w.retry_after("ns", "a", Duration::from_secs(300));
        assert!(w.forget("ns", "a"));
        assert!(w.due_in("ns", "a").is_none());
    }

    /// A finalized RestEndPoint which was successfully reconciled just now
    fn just_reconciled(check_frequency: u64) -> RestEndPoint {
        let mut rep = RestEndPoint::test().finalized();
        rep.spec.check_frequency = Some(check_frequency);
        rep.with_status(RestEndPointStatus {
            conditions: vec![ApplicationCondition::is_ready("done")],
            generation: 1,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn a_wakeup_goes_through_the_status_guard() {
        let (testctx, fakeserver, _) = Context::test();
        let restendpoint = just_reconciled(3600);
        testctx.wakeups.insert("default", "test");
        let mocksrv = fakeserver.run(Scenario::StatusPatch(restendpoint.clone()));
        reconcile(Arc::new(restendpoint), testctx.clone())
            .await
            .expect("reconciler");
        timeout_after_1s(mocksrv).await;
        assert!(testctx.wakeups.due_in("default", "test").is_none());
    }

    #[tokio::test]
    async fn the_first_reconcile_of_a_watching_endpoint_goes_through_the_status_guard() {
        let (testctx, fakeserver, _) = Context::test();
        let mut restendpoint = just_reconciled(3600);
        restendpoint.spec.watch_inputs = Some(true);
        let restendpoint = Arc::new(restendpoint);
        let mocksrv = fakeserver.run(Scenario::StatusPatch((*restendpoint).clone()));
        reconcile(restendpoint.clone(), testctx.clone())
            .await
            .expect("reconciler");
        timeout_after_1s(mocksrv).await;
        // only the first one: the mock apiserver is gone, a full reconcile would fail
        reconcile(restendpoint, testctx.clone()).await.expect("guarded");
        testctx.wakeups.remove("default", "test");
        assert!(testctx.wakeups.first_sight("default", "test"));
    }

    #[tokio::test]
    async fn a_failed_wakeup_is_retried() {
        let (testctx, fakeserver, _) = Context::test();
        drop(fakeserver); // every call to the apiserver fails
        let restendpoint = Arc::new(just_reconciled(0));
        testctx.wakeups.insert("default", "test");
        assert!(reconcile(restendpoint.clone(), testctx.clone()).await.is_err());
        assert!(testctx.wakeups.due_in("default", "test").is_some());
        // an event coming before the retry is due must not forget about it
        let action = reconcile(restendpoint, testctx.clone()).await.expect("guarded");
        assert_ne!(action, Action::await_change());
        assert!(testctx.wakeups.due_in("default", "test").is_some());
    }

    #[test]
    fn input_changed_wakes_the_endpoints_using_the_object() {
        use super::{InputKind, RESTPATH_FINALIZER, Wakeups, input_changed};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ManagedFieldsEntry, Time};
        use kube::{
            api::ObjectMeta,
            runtime::{reflector, watcher::Event},
        };
        let (store, mut writer) = reflector::store::<RestEndPoint>();
        let wakeups = Wakeups::default();
        let mut meta = ObjectMeta {
            name: Some("sec".into()),
            namespace: Some("ns1".into()),
            ..Default::default()
        };
        let mut apply = |r: Arc<RestEndPoint>| writer.apply_watcher_event(&Event::Apply((*r).clone()));
        apply(rep_with("off", "ns1", false, Some(("sec", None)), None));
        assert!(input_changed(InputKind::Secret, &meta, &store, &wakeups).is_empty());
        apply(rep_with("on", "ns1", true, Some(("sec", None)), None));
        let woken = input_changed(InputKind::Secret, &meta, &store, &wakeups);
        assert_eq!(woken.len(), 1);
        assert_eq!(woken[0].name, "on");
        assert!(wakeups.take_due("ns1", "on"));
        meta.managed_fields = Some(vec![ManagedFieldsEntry {
            manager: Some(RESTPATH_FINALIZER.into()),
            time: Some(Time("2026-01-01T00:00:00Z".parse().unwrap())),
            ..Default::default()
        }]);
        assert!(input_changed(InputKind::Secret, &meta, &store, &wakeups).is_empty());
        assert!(!wakeups.take_due("ns1", "on"));
    }

    #[test]
    fn our_own_writes_do_not_wake_anybody() {
        use super::{RESTPATH_FINALIZER, written_by_us};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ManagedFieldsEntry, Time};
        use kube::api::ObjectMeta;
        let entry = |manager: &str, ts: &str| ManagedFieldsEntry {
            manager: Some(manager.into()),
            time: Some(Time(ts.parse().unwrap())),
            ..Default::default()
        };
        let mut meta = ObjectMeta::default();
        assert!(!written_by_us(&meta), "no managedFields");
        meta.managed_fields = Some(vec![
            entry(RESTPATH_FINALIZER, "2026-01-01T00:00:00Z"),
            entry("kubectl", "2026-01-02T00:00:00Z"),
        ]);
        assert!(!written_by_us(&meta), "somebody else wrote last");
        meta.managed_fields = Some(vec![
            entry("kubectl", "2026-01-01T00:00:00Z"),
            entry(RESTPATH_FINALIZER, "2026-01-02T00:00:00Z"),
        ]);
        assert!(written_by_us(&meta), "kuberest wrote last");
        meta.managed_fields = Some(vec![entry(RESTPATH_FINALIZER, "2026-01-01T00:00:00Z")]);
        assert!(written_by_us(&meta), "only kuberest wrote");
        for order in [[RESTPATH_FINALIZER, "kubectl"], ["kubectl", RESTPATH_FINALIZER]] {
            meta.managed_fields = Some(order.map(|m| entry(m, "2026-01-01T00:00:00Z")).to_vec());
            assert!(!written_by_us(&meta), "same second: the event is kept");
        }
    }
}
