use std::{
    borrow::Borrow,
    collections::HashMap,
    fmt::{Debug, Formatter},
    iter,
    path::Path,
    str::FromStr,
    sync::{
        Arc, LazyLock, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
        mpsc::{self, Receiver, Sender, SyncSender},
    },
};

use anyhow::bail;
use nu_plugin::{EngineInterface, MsgPackSerializer, Plugin, PluginCommand};
use nu_protocol::{
    CustomValue, FromValue, IntoValue, LabeledError, ShellError, Span, Spanned, Type, Value,
    ast::Operator,
};
use petname::petname;
use serde::{Deserialize, Serialize};
use tap::Pipe as _;
use thiserror::Error;
use uuid::Uuid;

use crate::cmds::{drop::ClosePipeCmd, get_pipe::GetPipeCmd, list::ListPipesCmd, new::NewPipeCmd, read::ReadFromPipeCmd, write::WriteIntoPipeCmd};

pub mod pipe_arg;

pub const DEFAULT_PIPE_SIZE: usize = 256_usize;

fn main() {
    // PipePlugin::new().
    nu_plugin::serve_plugin(&PipePlugin::new(), MsgPackSerializer);
}

impl Plugin for PipePlugin {
    fn version(&self) -> String {
        "0.1.0+0.113.1".to_string()
    }

    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
    	Vec::from_iter([
   			Box::new(NewPipeCmd) as _ ,
    		Box::new(GetPipeCmd) as _ ,
    		Box::new(ListPipesCmd) as _ ,
    		Box::new(ClosePipeCmd) as _ ,
    		Box::new(ReadFromPipeCmd) as _ ,
      		Box::new(WriteIntoPipeCmd) as _
    	])
    }
}

// impl Debug for PipeInletId {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         f.debug_tuple("PipeInlet").field(&self.0).finish()
//     }
// }

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, FromValue, IntoValue)]
// #[serde(transparent)]
pub struct PipeId(String);
impl Borrow<str> for PipeId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PipeId {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    //this should not be here
    pub fn new_random() -> Self {
        petname(2_u8, "-")
            .expect("failed to create a name")
            .pipe(PipeId)
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, IntoValue, FromValue)]
pub struct PipeWriterValue(PipeId);

unsafe impl Send for PipeWriterValue {}

impl PipeWriterValue {
    pub fn type_name() -> &'static str {
        stringify!(PipeSender)
    }
}

impl PipeReaderValue {
    pub fn type_name() -> &'static str {
        stringify!(PipeOutput)
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, FromValue, IntoValue)]
pub struct PipeReaderValue(PipeId);

impl Debug for PipeReaderValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(stringify!(PipeOutput))
            .field(&self.0)
            .finish()
    }
}

pub struct Pipe {
    sender: SyncSender<Value>,
    receiver: Arc<Mutex<Receiver<Value>>>,
}

impl Pipe {
    pub fn new(size: usize) -> Pipe {
        let (sender, receiver) = mpsc::sync_channel(size);

        Pipe {
            sender: sender,
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }

    pub fn get_writer(&self) -> PipeWriter {
        PipeWriter(self.sender.clone())
    }

    pub fn get_receiver(&self) -> PipeReader {
        PipeReader(self.receiver.clone())
    }
}

#[derive(Clone)]
pub struct PipeWriter(SyncSender<Value>);

#[derive(Clone)]
pub struct PipeReader(Arc<Mutex<Receiver<Value>>>);

pub trait PipePort {}

impl PipeWriter {
    pub fn write_one(&mut self, value: Value) -> Result<(), anyhow::Error> {
        self.0.send(value)?;
        Ok(())
    }

}

impl PipeReader {
    pub fn read_one(&mut self) -> Result<Value, anyhow::Error> {
        self.0
            .lock()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .recv()?
            .pipe(Ok)
    }
    pub fn read(self) -> impl Iterator<Item = Value> {
        let mut opt_self = Some(self);
        iter::from_fn(move || {
            let Some(inner) = &mut opt_self else {
                return None;
            };
            match inner.read_one() {
                Ok(val) => {
                	return Some(val);
                },
                Err(e) => {
                	drop(inner);
                 	opt_self = None;
	                return None;
                },
            }
        })
    }
}

pub struct PipePlugin {
    state: Arc<RwLock<PipeState>>,
}

impl PipePlugin {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(PipeState::new())),
        }
    }
    pub fn state(&self) -> Result<RwLockReadGuard<'_, PipeState>, LabeledError> {
        let state = self
            .state
            .read()
            .map_err(|e| LabeledError::new("Error when trying to lock state for reading."))?;
        Ok(state)
    }
    pub fn state_mut(&self) -> Result<RwLockWriteGuard<'_, PipeState>, LabeledError> {
        let state = self
            .state
            .write()
            .map_err(|_| LabeledError::new("Error when trying to state."))?;
        Ok(state)
    }
}

pub struct PipeState {
    pipes: HashMap<PipeId, Pipe>,
}

impl PipeState {
    pub fn new() -> Self {
        Self {
            pipes: Default::default(),
        }
    }

    #[inline(always)]
    pub fn pipe_exists(&self, id: &impl Borrow<str>) -> bool {
        self.pipes.contains_key(id.borrow())
    }

    // this wont check for existing names, and WILL KILL ANY ALREADY EXISTING VALUE
    pub fn new_pipe(&mut self, name: impl Into<String>, size: usize) -> PipeId {
        let id = PipeId(name.into());
        let new_pipe = Pipe::new(size);
        self.pipes.insert(id.clone(), new_pipe);

        id
    }

    pub fn get_pipe_value<'this, 'key>(&'this self, id: &'key PipeId) -> Option<PipeValue> {
        if !self.pipes.contains_key(id) {
            return None;
        }

        Some(PipeValue(id.clone()))
    }

    pub fn get_pipe<'this, 'key>(&'this self, id: &'key PipeId) -> Option<&'this Pipe> {
        self.pipes.get(id)
    }

    pub fn drop_pipe(&mut self, id: &PipeId) -> ObjectRemovalResult {
        self.pipes
            .remove(id)
            .pipe(ObjectRemovalResult::from_removed)
    }

    pub fn list_pipes<'a>(&'a self) -> impl Iterator<Item = &'a PipeId> {
        self.pipes.keys()
    }
}

#[derive(Debug, Error)]
#[error("pipe name already exists")]
pub struct PipeNameExistsError {
    pub name: String,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ObjectRemovalResult {
    DoesntExist,
    Removed,
}

impl ObjectRemovalResult {
    pub fn from_removed<T>(removed: Option<T>) -> Self {
        match removed {
            Some(_) => Self::Removed,
            None => Self::DoesntExist,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, IntoValue, FromValue)]
pub struct PipeValue(PipeId);
impl PipeValue {
    #[inline(always)]
    pub fn new(id: PipeId) -> Self {
        Self(id)
    }
    pub fn into_sender(self) -> PipeWriterValue {
        PipeWriterValue(self.0)
    }

    pub fn into_receiver(self) -> PipeReaderValue {
        PipeReaderValue(self.0)
    }
}

pub mod cmds;

pub trait GetsPipe {
    fn pipe_id(&self) -> &PipeId;
}

impl GetsPipe for PipeValue {
    fn pipe_id(&self) -> &PipeId {
        &self.0
    }
}

impl GetsPipe for PipeWriterValue {
    fn pipe_id(&self) -> &PipeId {
        &self.0
    }
}

impl GetsPipe for PipeReaderValue {
    fn pipe_id(&self) -> &PipeId {
        &self.0
    }
}

pub trait GetsPipeSender: GetsPipe {}
pub trait GetsPipeOutput: GetsPipe {}

impl GetsPipeSender for PipeValue {}
impl GetsPipeSender for PipeWriterValue {}
impl GetsPipeOutput for PipeValue {}
impl GetsPipeOutput for PipeReaderValue {}

pub struct NameGenerator {
    words: u8,
    sep: String,
}

pub const RANDOM_PIPE_NAME_GENERATOR: LazyLock<NameGenerator> = LazyLock::new(|| NameGenerator {
    words: 2,
    sep: "-".to_string(),
});

impl NameGenerator {
    pub fn generate(&self) -> String {
        petname::petname(self.words, &self.sep).unwrap()
    }
    pub fn generate_distinct(
        &self,
        mut name_is_taken_checker: impl FnMut(&String) -> bool,
    ) -> String {
        loop {
            let new = self.generate();
            let name_is_taken = name_is_taken_checker(&new);
            if name_is_taken {
                continue;
            }
            return new;
        }
    }
}
