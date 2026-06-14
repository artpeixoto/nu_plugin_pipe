use std::{collections::HashMap, fmt::{Debug, Formatter}, path::Path, sync::{Arc, Mutex, RwLock, mpsc::{self, Receiver, Sender}}};

use nu_plugin::{EngineInterface, Plugin, PluginCommand};
use nu_protocol::{
    CustomValue, FromValue, LabeledError, ShellError, Span, Spanned, Type, Value, ast::Operator,
};
use petname::petname;
use serde::{Deserialize, Serialize};
use tap::Pipe;
use uuid::Uuid;

fn main() {

}




impl Plugin for StreamPlugin {
    fn version(&self) -> String {
        "0.1.0+0.113.1".to_string()
    }


    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        todo!()
    }
}



impl Debug for StreamInletId{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("StreamInlet").field(&self.0).finish()
    }
}
pub struct MakeStreamCmd;



#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct  StreamId(Str) ;

impl StreamId{
	pub fn new(name: impl Into<Str>) -> Self{
		Self(name.into())
	}

	pub fn new_random() -> Self{
		petname(2_u8, "-")
		.expect("failed to create a name")
		.into_boxed_str()
		.pipe(StreamId)
	}
}



pub struct StreamReceiverId(StreamId);

impl Debug for StreamReceiverId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Streamreceiver").field(&self.0).finish()
    }
}

pub struct Stream{
	sender: Sender<Value>,
	receiver: Arc<Mutex<Receiver<Value>>>,
}

impl Stream{
	pub fn new() -> Stream {
		let (sender, receiver) = mpsc::channel();

		Stream {
			sender: sender,
			receiver: Arc::new(Mutex::new(receiver))
		}
	}
	pub fn get_inlet(&self) -> StreamSender{
		StreamSender(self.sender.clone())
	}
}


pub struct StreamSender( Sender<Value> );
pub struct StreamReceiver( Arc<Mutex<Receiver<Value>>> );


impl Stream{


}



pub struct StreamPlugin{
	streams: HashMap<
		StreamId, Stream>
}
impl StreamPlugin{
	pub fn new() -> Self{
		Self{streams: Default::default()}
	}

	pub fn new_stream(&mut self, name: Option<impl Into<Str>>) -> Result<StreamId, anyhow::Error> {
		let name = match name{
			Some(name) => {
				let id = StreamId::new(name);
				if self.streams.contains_key(id) {

				}
			} ,
			None => {
				loop {
					let new_name = StreamId

				}
			},
		};

		let stream = Stream::new();
		self.streams.insert(id.clone(), stream);
		id
	}


	pub fn get_stream(&mut self, )

}

pub type Str = Box<str>;
