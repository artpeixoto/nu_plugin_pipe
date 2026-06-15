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
        let signature = Signature::new(self.name())
            .optional(
                "name",
                SyntaxShape::String,
                "The name of the new fucking pipe. If left empty, a random one will be generated.",
            )
            .named(
                "size",
                SyntaxShape::Int,
                "The size of the buffer. The default is 256.",
                Some('s'),
            )
            .input_output_type(Type::Nothing, PipeValue::expected_type());

        signature
    }

    fn description(&self) -> &str {
        "Creates a new pipe"
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

        let name = {
            match call.opt::<String>(0)? {
                Some(name) => {
                    if state.pipe_exists(&name) {
                        return Err(LabeledError::new("a pipe with that name already exists"));
                    };
                    name
                }
                None => {
                    RANDOM_PIPE_NAME_GENERATOR.generate_distinct(|name| state.pipe_exists(name))
                }
            }
        };

        let size = call
            .get_flag::<i64>("length")?
            .map(|x| x as usize)
            .unwrap_or(DEFAULT_PIPE_SIZE);


       let pipe_value = state.new_pipe(name, size).pipe(PipeValue::new);
       let result = pipe_value.into_value(Span::unknown()).into_pipeline_data();

       Ok(result)
    }
}
