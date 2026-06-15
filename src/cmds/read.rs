use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{IntoInterruptiblePipelineData, LabeledError, ListStream, PipelineData, Signals, Signature, Span, Type};
use tap::Pipe;

use crate::{PipePlugin, pipe_arg::PipeArgAllowedTypes};

pub struct ReadFromPipeCmd;
const PIPE_ARG_ALLOWED_TYPES: PipeArgAllowedTypes = PipeArgAllowedTypes::all().only_readers();

impl PluginCommand for ReadFromPipeCmd {
    type Plugin = PipePlugin;

    fn name(&self) -> &str {
        "pipe read"
    }

    fn signature(&self) -> Signature {
        Signature::new(self.name())
            .required(
                "pipe",
                PIPE_ARG_ALLOWED_TYPES.arg_type().to_shape(),
                "the pipe from which you wish to read a value",
            )
            .input_output_type(Type::Nothing, Type::list(Type::Any))
    }

    fn description(&self) -> &str {
        "receives values streaming from the fucking pipe"
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

        let pipe_receiver = plugin
            .state()?
            .get_pipe(&pipe_id)
            .ok_or_else(|| LabeledError::new("pipe does not exist or is closed"))?
            .get_receiver()
            ;

        let reader = pipe_receiver.read();

        ListStream::new(
	        Box::new(reader),
	        Span::unknown(),
	        engine.signals().clone()
        )
        .pipe(|stream| PipelineData::list_stream(stream, None) )
        .pipe(Ok)
    }
}
