use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    FromValue, IntoPipelineData, IntoValue, LabeledError, PipelineData, PipelineMetadata, Record,
    ShellError, Signature, Span, SyntaxShape, Type, Value,
    engine::{self, Call, Command, EngineState, Stack},
};
use tap::Pipe;

use crate::{DEFAULT_PIPE_SIZE, PipePlugin, PipeValue, RANDOM_PIPE_NAME_GENERATOR};

#[derive(Clone, Debug)]
pub struct NewPipeCmd;

impl PluginCommand for NewPipeCmd {
    fn name(&self) -> &str {
        "pipe new"
    }

    fn signature(&self) -> Signature {
        let signature =
        	Signature::new(self.name())
         	.description("
            ")
            .optional(
                "name",
                SyntaxShape::String,
                "The name of the new pipe. If left empty, a random one will be generated.",
            )
            .named(
                "size",
                SyntaxShape::Int,
                format!("The size of the buffer. The default is {DEFAULT_PIPE_SIZE}."),
                Some('s'),
            )
            .named(
            	"error-on-clash",
           		SyntaxShape::Boolean,
             	"Determines whether an error will be thrown if you try to create a pipe with that name. If you decide to not throw, the existing pipe will be returned. By default, no error will be returned, for convenience.",
              	Some('e')
            )
            .input_output_type(Type::Nothing, PipeValue::expected_type());

        signature
    }

    fn description(&self) -> &str {
        " Creates and registers a new named pipe to the system and returns its name. If not given a name, a random name will be generated.
        "
    }

    type Plugin = PipePlugin;

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call  : &EvaluatedCall,
        input : PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let mut state = plugin.state_mut()?;

        let error_on_existing =
        	call
         	.get_flag::<bool>("clash-on-error")?
         	.unwrap_or(false);

        let name = {
            match call.opt::<String>(0)? {
                Some(name) => {
                    if state.pipe_exists(&name) && error_on_existing {
                        return Err(LabeledError::new("a pipe with that name already exists"));
                    } else {
                    	name
                    }
                }
                None => {
                    RANDOM_PIPE_NAME_GENERATOR.generate_distinct(|name| state.pipe_exists(name))
                }
            }
        };

        let size = call
            .get_flag::<i64>("size")?
            .map(|x| x as usize)
            .unwrap_or(DEFAULT_PIPE_SIZE);


        let pipe_value 	= state.new_pipe(name, size).pipe(PipeValue::new);
        let result 		= pipe_value.into_value(Span::unknown()).into_pipeline_data();

        Ok(result)
    }
}
