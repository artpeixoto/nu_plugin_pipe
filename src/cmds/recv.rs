use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    IntoInterruptiblePipelineData, IntoSpanned, LabeledError, ListStream, PipelineData,
    PipelineMetadata, Signals, Signature, Span, SyntaxShape, Type, Value, engine::Closure,
};
use tap::Pipe;

use crate::{PipePlugin, PipeReaderReadCondition, pipe_arg::PipeArgAllowedTypes};

pub struct RecvFromPipeCmd;
const PIPE_ARG_ALLOWED_TYPES: PipeArgAllowedTypes = PipeArgAllowedTypes::all().only_readers();

impl PluginCommand for RecvFromPipeCmd {
    type Plugin = PipePlugin;

    fn name(&self) -> &str {
        "pipe recv"
    }

    fn signature(&self) -> Signature {
        Signature::new(self.name())
            .required(
                "pipe",
                PIPE_ARG_ALLOWED_TYPES.arg_type().to_shape(),
                "the pipe from which you wish to read a value",
            )
            .input_output_type(Type::Nothing, Type::Any)
            .category(nu_protocol::Category::Generators)
    }

    fn description(&self) -> &str {
        "Receives a single value from the named pipe"
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let pipe_id = {
            let value = call
                .nth(0)
                .ok_or(LabeledError::new("Missing pipe argument"))?;
            PIPE_ARG_ALLOWED_TYPES.parse_value(value)?
        };

        let mut pipe_receiver = plugin
            .state()?
            .get_pipe(&pipe_id)
            .ok_or_else(|| LabeledError::new("pipe does not exist or is closed"))?
            .get_receiver();

        // pipe_receiver.read().into_pipeline_data(call.head, engine.signals().clone()).pipe(Ok)
        let mut pipe_receiver_lock = pipe_receiver.0.blocking_lock();
        let value = pipe_receiver_lock.recv().ok();

        match value {
            None => {
                plugin.state_mut()?.drop_pipe(&pipe_id);
                return PipelineData::Empty.pipe(Ok);
            }
            Some(value) => {
                return PipelineData::value(value, None).pipe(Ok);
            }
        }
    }
}

pub struct ReadFromPipeCmd;
impl PluginCommand for ReadFromPipeCmd {
    type Plugin = PipePlugin;

    fn name(&self) -> &str {
        "pipe read"
    }

    fn signature(&self) -> Signature {
        Signature::new(self.name())
            .description(self.description())
            .required(
                "pipe",
                PIPE_ARG_ALLOWED_TYPES.arg_type().to_shape(),
                "the pipe from which you wish to read",
            )
            .optional(
                "condition",
                Type::one_of([Type::Int, Type::Closure, Type::Nothing, ]).to_shape(),
                " A condition determining how much you wish to read.\n - If set to a null, it will take values until the pipe is closed.\n - If set to a number, it will take that amount of values.\n - If set to a closure, it will run that closure peeking each value. The result of the closure will determine whether the reading should continue.
                ",
            )

            .input_output_type(Type::Nothing, Type::list(Type::Any))
            .category(nu_protocol::Category::Generators)
    }

    fn description(&self) -> &str {
        "Receives values from the named pipe.  It is important to note that, because of the way the nu engine works, this reader is STILL A BIT PROBLEMATIC AND WILL READ THE ENTIRE PIPE, EVEN IF YOU JUST WANT TO TAKE THE PART OF THE ELEMENTS. To help with that, an additional condition argument can be passed to determine when to stop taking items with precision."
    }
    // fn get_dynamic_completion(
    //     &self,
    //     plugin: &Self::Plugin,
    //     engine: &EngineInterface,
    //     call: nu_plugin::DynamicCompletionCall,
    //     arg_type: nu_protocol::engine::ArgType,
    //     _experimental: nu_protocol::engine::ExperimentalMarker,
    // ) -> Option<Vec<nu_protocol::DynamicSuggestion>> {

    // }

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let pipe_id = {
            let value = call
                .nth(0)
                .ok_or(LabeledError::new("Missing pipe argument"))?;
            PIPE_ARG_ALLOWED_TYPES.parse_value(value)?
        };

        let condition = {
            match call
                .opt::<Value>(1)?
                .unwrap_or(Value::nothing(Span::unknown()))
            {
                Value::Int {
                    val, internal_span, ..
                } if val >= 0 => Some(PipeReaderReadCondition::Count(val as usize)),
                Value::Int {
                    val, internal_span, ..
                } if val < 0 => {
                    return Err(LabeledError::new("Condition number cant be negative"));
                }
                // Value::Float { val, internal_span , ..} => todo!(),
                Value::Closure {
                    val: closure,
                    internal_span,
                    ..
                } => Some(PipeReaderReadCondition::Filter({
                    let engine = engine.clone();
                    let closure = closure.as_ref().clone().into_spanned(internal_span);
                    let inner = Box::new(move |val_peek: &Value| {
                        let val = val_peek.clone();
                        let output = engine.eval_closure(&closure, vec![val.clone()], Some(val));
                        Ok(output?.as_bool()?)
                    });
                    inner
                })),
                Value::Nothing { internal_span, .. } => None,
                a => {
                    return Err(LabeledError::new("Condition has invalid type."));
                } // Value::Glob { val, no_expand, internal_span } => todo!(),
                  // Value::Filesize { val, internal_span } => todo!(),
                  // Value::Duration { val, internal_span } => todo!(),
                  // Value::Date { val, internal_span } => todo!(),
                  // Value::Range { val, signals, internal_span } => todo!(),
                  // Value::Record { val, internal_span } => todo!(),
                  // Value::List { vals, signals, internal_span } => todo!(),
                  // Value::Error { error, internal_span } => todo!(),
                  // Value::Binary { val, internal_span } => todo!(),
                  // Value::CellPath { val, internal_span } => todo!(),
                  // Value::Custom { val, internal_span } => todo!(),
            }
        };

        let mut pipe_receiver = plugin
            .state()?
            .get_pipe(&pipe_id)
            .ok_or_else(|| LabeledError::new("pipe does not exist or is closed"))?
            .get_receiver()
            .read(condition);

        ListStream
        	::new(pipe_receiver, call.head, Signals::empty())
            .pipe(|ls| PipelineData::list_stream(ls, None))
            .pipe(Ok)

        // pipe_receiver.read().into_pipeline_data(call.head, engine.signals().clone()).pipe(Ok)
        // let mut pipe_receiver_lock = pipe_receiver.0.blocking_lock();
        // let value = pipe_receiver_lock.recv().ok();

        // match value {
        //     None => {
        //         plugin.state_mut()?.drop_pipe(&pipe_id);
        //         return PipelineData::Empty.pipe(Ok);
        //     }
        //     Some(value) => {
        //         return PipelineData::value(value, None).pipe(Ok);
        //     }
        // }
    }
}
