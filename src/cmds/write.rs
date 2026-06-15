use std::thread;

use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    IntoInterruptiblePipelineData, LabeledError, ListStream, PipelineData, Signals, Signature,
    Span, Type,
};
use tap::Pipe;

use crate::{PipePlugin, pipe_arg::PipeArgAllowedTypes};

pub struct WriteIntoPipeCmd;
const PIPE_ARG_ALLOWED_TYPES: PipeArgAllowedTypes = PipeArgAllowedTypes::all().only_writers();

impl PluginCommand for WriteIntoPipeCmd {
    type Plugin = PipePlugin;

    fn name(&self) -> &str {
        "pipe write"
    }

    fn signature(&self) -> Signature {
        Signature::new(self.name())
            .required(
                "pipe",
                PIPE_ARG_ALLOWED_TYPES.arg_type().to_shape(),
                "the pipe into which to write",
            )
            .input_output_type(Type::List(Box::new(Type::Any)), Type::Nothing)

    }

    fn description(&self) -> &str {
        "writes values streaming to the pipe"
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

        let mut pipe_writer = plugin
            .state()?
            .get_pipe(&pipe_id)
            .ok_or_else(|| LabeledError::new("pipe does not exist or is closed"))?
            .get_writer();

        let a = input
            .into_iter()
            .try_for_each(move |val| pipe_writer.write_one(val))
            .map_err(|e| {
                LabeledError::new("something happened. Probably the pipe was closed somewhere.")
            })?;

        Ok(PipelineData::Empty)
    }
}
