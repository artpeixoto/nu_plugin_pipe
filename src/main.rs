use std::{
    borrow::Borrow,
    collections::HashMap,
    fmt::{Debug, Error, Formatter},
    iter::{self, Peekable, from_fn},
    path::Path,
    str::FromStr,
    sync::{
        Arc, LazyLock, RwLock, RwLockReadGuard, RwLockWriteGuard,
        mpsc::{self, Receiver, RecvError, Sender, SyncSender, TryRecvError},
    },
    thread::spawn,
};

use anyhow::bail;
use nu_plugin::{EngineInterface, JsonSerializer, MsgPackSerializer, Plugin, PluginCommand};
use nu_protocol::{
    CustomValue, FromValue, IntoValue, LabeledError, ShellError, SignalAction, Span, Spanned, Type,
    Value, ast::Operator, engine,
};
use petname::petname;
use serde::{Deserialize, Serialize};
use tap::{Conv, Pipe as _};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::cmds::{
    drop::ClosePipeCmd,
    list::ListPipesCmd,
    new::NewPipeCmd,
    recv::{ReadFromPipeCmd, RecvFromPipeCmd},
    write::WriteIntoPipeCmd,
};

pub mod pipe_arg;

pub const DEFAULT_PIPE_SIZE: usize = 32_usize;

// ok i gotta admit this crate is not organized at all. Later ill tidy things up a bit.

fn main() {
    // PipePlugin::new().
    nu_plugin::serve_plugin(&PipePlugin::new(), JsonSerializer);
}

impl Plugin for PipePlugin {
    fn version(&self) -> String {
        "0.0.1+0.113.1".to_string()
    }

    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        Vec::from_iter([
            Box::new(NewPipeCmd) as _,
            Box::new(ListPipesCmd) as _,
            Box::new(ClosePipeCmd) as _,
            Box::new(RecvFromPipeCmd) as _,
            Box::new(ReadFromPipeCmd) as _,
            Box::new(WriteIntoPipeCmd) as _,
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

// #[derive(Serialize, Deserialize, Clone, PartialEq, Eq, IntoValue, FromValue)]
// pub struct PipeWriterValue(PipeId);

// unsafe impl Send for PipeWriterValue {}

// impl PipeWriterValue {
//     pub fn type_name() -> &'static str {
//         stringify!(PipeSender)
//     }
// }

// impl PipeReaderValue {
//     pub fn type_name() -> &'static str {
//         stringify!(PipeOutput)
//     }
// }

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
    tx: PipeInnerSender,
    rx: PipeInnerReceiver,
}

pub type PipeInnerSender = SyncSender<Value>;
pub type PipeInnerReceiver = Arc<Mutex<PeekableReceiver<Value>>>;

impl Pipe {
    pub fn new(size: usize) -> Pipe {
        let (sender, receiver) = mpsc::sync_channel(size);
        let receiver = receiver
            .pipe(PeekableReceiver::new)
            .pipe(Mutex::new)
            .pipe(Arc::new);

        Pipe {
            tx: sender,
            rx: receiver,
        }
    }

    pub fn get_writer(&self) -> PipeWriter {
        PipeWriter(self.tx.clone())
    }

    pub fn get_receiver(&self) -> PipeReader {
        PipeReader(self.rx.clone())
    }
}

pub struct PeekableReceiver<T> {
    current_value: Option<T>,
    inner: Receiver<T>,
}

impl<T> PeekableReceiver<T> {
    pub fn new(inner: Receiver<T>) -> Self {
        Self {
            current_value: None,
            inner,
        }
    }

    pub fn recv(&mut self) -> Result<T, RecvError> {
        if let Some(current) = self.current_value.take() {
            return Ok(current);
        }
        self.inner.recv()
    }

    pub fn peek(&mut self) -> Result<&T, RecvError> {
        if self.current_value.is_none() {
            let val = self.inner.recv()?;
            self.current_value = Some(val);
        }

        self.current_value.as_ref().unwrap().pipe(Ok)
    }
}

pub struct PipeWriter(PipeInnerSender);

pub struct PipeReader(PipeInnerReceiver);

impl PipeWriter {
    pub fn write_one(&mut self, value: Value) -> Result<(), anyhow::Error> {
        self.0.send(value)?;
        Ok(())
    }
}

pub enum PipeReaderReadCondition {
    Count(usize),
    Filter(Box<dyn FnMut(&Value) -> Result<bool, ShellError> + Send + Sync + 'static>),
}

impl PipeReader {
    pub fn read_one(&mut self) -> Result<Value, anyhow::Error> {
        self.0.blocking_lock().recv()?.pipe(Ok)
    }
    pub fn read(
        self,
        mut condition: Option<PipeReaderReadCondition>,
    ) -> impl Iterator<Item = Value> {
        let mut lock = self.0.blocking_lock_owned();
        enum Either3<T, U, V> {
            A(T),
            B(U),
            C(V),
        }
        impl<T, U, V, I> Iterator for Either3<T, U, V>
        where
            T: Iterator<Item = I>,
            U: Iterator<Item = I>,
            V: Iterator<Item = I>,
        {
            type Item = I;

            #[inline(always)]
            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    Either3::A(a) => a.next(),
                    Either3::B(b) => b.next(),
                    Either3::C(c) => c.next(),
                }
            }
        }

        // let base_iter =
        // 	from_fn(move || lock.recv().ok())
        //  .fuse();

        use Either3::*;

        let iter = match condition {
            None => A(from_fn({
                let mut lock_cell = Some(lock);
                move || {
                    let mut lock = lock_cell.take()?;
                    let val = lock.recv().ok()?;
                    lock_cell.insert(lock);
                    return Some( val )
                }
            })),
            Some(PipeReaderReadCondition::Count(n)) => {
            	let mut lock_cell = Some(lock);
             	let mut count = n;
                let inner = move || {
                    let mut lock = lock_cell.take()?;

                    if count == 0 { return None; }
                    let val = lock.recv().ok()?;
                    count -= 1;
                    lock_cell.insert(lock);
                    return Some( val )
                };

                B(from_fn( inner ))
            }
            Some(PipeReaderReadCondition::Filter(mut filter)) => {
                let inner = from_fn({
                	let mut lock_cell = Some(lock);
                    move || {
                    	let mut lock = lock_cell.take()?;
                        let next = lock.peek().ok()?;
                        let filter_res = (filter)(next);
                        match filter_res {
                            Ok(filter_res) => {
                                if filter_res {
                                    let val = lock.recv().ok()?;
                                    lock_cell.insert(lock);
                                    Some(val)
                                } else {
                                    None
                                }
                            }
                            Err(err) => {
                                let err = Value::error(err, Span::unknown());
                                Some(err)
                            }
                        }
                    }
                })
                ;
                C(inner)
            }
        };

        #[cfg(feature = "testing")]
        let iter = iter.map(|v| {
        	println!("{v:?}");
         	return v;
        });

        return iter;
    }

    //   pub fn read(self, engine: EngineInterface, mut condition: Option<PipeReaderReadCondition>)
    //   	-> Result<impl Iterator<Item = Value>, anyhow::Error>
    //   {
    // 		let lock = self.0.blocking_lock_owned();

    // //  		enum MasterSignalMsg{
    // //    		Interrupt,
    // //    	}

    // //     	enum MasterMsg{
    // // 	Signal(MasterSignalMsg),
    // //  	Reader(Value)
    // // };

    // // enum MasterReaderMsg{
    // // 	Read{
    // // 		val: Value,
    // // 		peeked: bool
    // // 	},
    // // 	PipeClosed,
    // // 	Error(anyhow::Error)
    // // }

    // // enum ReaderCommand{
    // // 	Read{ peek: bool },
    // // 	Stop,
    // // }

    // // let ( reader_command_sender, reader_command_receiver ) = mpsc::sync_channel::<ReaderCommand>(1);

    // // let ( master_sender, master_receiver ) = mpsc::channel();

    // // let reader_handle = spawn({
    // // 	let pipe = Some(lock);
    // // 	let master_sender = master_sender.clone();
    // // 	move ||  { 'READER_LOOP: loop {
    // // 		let Ok(command) = reader_command_receiver.recv() else {break 'READER_LOOP};
    // // 		match command {
    // //    				ReaderCommand::Stop => { break 'READER_LOOP },
    // // 	        ReaderCommand::Read { peek } => {
    // // 				if (peek) {
    // // 					pipe.
    // // 				}
    // // 			},
    // // 	    }
    // // 	} }
    // // });

    // // let signal_receiver_guard = engine.register_signal_handler(Box::new(
    // // 	{
    // // 		move |signal_action: SignalAction|{
    // // 			let _ = master_sender.send( MasterMsg::Signal(MasterSignalMsg::Interrupt) );
    // // 		}
    // // 	}
    // // ))?;

    // // let master  =
    // // 	iter::from_fn({
    // // 		let mut reader_handle = Some(reader_handle);
    // // 		let mut signal_receiver_guard = Some(signal_receiver_guard);
    // // 		let mut is_done = false;

    // // 		move || {
    // // 			if is_done {return None}

    // // 			let (value, should_continue) : (Option<Value>, bool)= (||  ->  Result<(Option<Value>, bool), anyhow::Error>{
    // // 				match master_receiver.try_recv() {
    // // 			        Ok(MasterMsg::Signal(MasterSignalMsg::Interrupt)) => return Ok((None, false)),
    // // 					Err(TryRecvError::Disconnected) => {return Ok((None, false))}
    // // 			        Ok(MasterMsg::Reader(value)) => {bail!("what the fuck")},
    // // 					Err(TryRecvError::Empty) => {()}
    // // 			    };

    // // 				match &mut condition {
    // //         			Some( PipeReaderReadCondition::Filter(_) ) => todo!(),
    // //         			Some( PipeReaderReadCondition::Count(_) ) => todo!(),
    // // 			        None => { // simple read
    // // 						let Ok(_) = reader_command_sender.send(ReaderCommand::Read { peek: false }) else {
    // // 							bail!("reader fucked up")
    // // 						};
    // // 						match master_receiver.recv()? {
    // // 				            MasterMsg::Signal(master_signal_msg) => todo!(),
    // // 				            MasterMsg::Reader(value) => todo!(),
    // // 				        }
    // // 					},
    // // 				}
    // // 			} ())
    // // 			.map_err(|e| eprintln!("Something bad happened in the master: {e:?}"))
    // // 			.unwrap_or(( None, false ));

    // // 			if (!should_continue) {
    // // 				if let Some(reader_handle_value) = reader_handle.take(){
    // // 					let _ = reader_command_sender.send(ReaderCommand::Stop);
    // // 				}
    // // 				if let Some(signal_receiver_guard) = signal_receiver_guard.take(){
    // // 					let _ = signal_receiver_guard;
    // // 				}
    // // 				is_done = true;
    // // 			}

    // // 			return value;
    // // 		}
    // // 	})
    // // 	.fuse();

    // lock.inner.into_iter()
    //   }
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

// impl GetsPipe for PipeWriterValue {
//     fn pipe_id(&self) -> &PipeId {
//         &self.0
//     }
// }

impl GetsPipe for PipeReaderValue {
    fn pipe_id(&self) -> &PipeId {
        &self.0
    }
}

pub trait GetsPipeSender: GetsPipe {}
pub trait GetsPipeOutput: GetsPipe {}

impl GetsPipeSender for PipeValue {}
// impl GetsPipeSender for PipeWriterValue {}
impl GetsPipeOutput for PipeValue {}
// impl GetsPipeOutput for PipeReaderValue {}

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
