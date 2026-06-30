use std::ops::Not;

use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{FromValue, LabeledError, PipelineData, Signature, SyntaxShape, Type, Value};
use tap::Pipe;


use crate::{
    PipeId, PipePlugin,  PipeValue,
    pipe_arg::{PipeArgAllowedTypes, pipe_arg_type, pipe_arg_from_value},
};

pub struct ClosePipeCmd;
const NO_ERROR_ON_NOT_FOUND_FLAG: &str = "no-error-on-not-found";
const PIPE_ARG_ALLOWED_TYPES: PipeArgAllowedTypes = PipeArgAllowedTypes::all();

impl PluginCommand for ClosePipeCmd {
    type Plugin = PipePlugin;

    fn name(&self) -> &str {
        "pipe close"
    }

    fn signature(&self) -> nu_protocol::Signature {
        Signature::new(self.name())
        .description("Closes a pipe. It does so by dropping it.")
        .required(
            "pipe",
            pipe_arg_type(&PIPE_ARG_ALLOWED_TYPES).to_shape(),
            "the pipe to close.",
        )
        .switch(
	        NO_ERROR_ON_NOT_FOUND_FLAG,
	        "if set, the command will not throw an error if the pipe is not found. For cases when you don't know whos gonna close it. ",
	        None
	    )
    }

    fn description(&self) -> &str {
        "it closes a pipe, such that nobody else can open it. "
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call  : &EvaluatedCall,
        input : PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let pipe_id = {
            let value = call
                .nth(0)
                .ok_or(LabeledError::new("Missing pipe argument"))?;

            pipe_arg_from_value(value, &PipeArgAllowedTypes::all())?
        };

        let errors_on_not_found =
        	call
            .get_flag::<bool>(NO_ERROR_ON_NOT_FOUND_FLAG)?
            .unwrap_or(false)
            .not();

        let mut state = plugin.state_mut()?;
        if !state.pipe_exists(&pipe_id) && errors_on_not_found{
        	return Err( LabeledError::new ("The pipe does not exist"))
        }
        state.drop_pipe(&pipe_id);

        Ok(PipelineData::Empty)
    }
}
