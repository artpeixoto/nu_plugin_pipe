use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    IntoPipelineData, IntoValue, LabeledError, PipelineData, PipelineMetadata, Record, ShellError,
    Signature, Span, SyntaxShape, Type, Value,
    engine::{self, Call, Command, EngineState, Stack},
};
use tap::Pipe;

use crate::PipePlugin;

#[derive(Clone, Debug)]
pub struct ListPipesCmd;

impl PluginCommand for ListPipesCmd {
    fn name(&self) -> &str {
        "pipe list"
    }

    fn signature(&self) -> Signature {
        Signature::new(self.name())
            .input_output_type(Type::Nothing, Type::List(Box::new(Type::String)))
        // .optional("new", SyntaxShape::Nothing, desc)
    }

    fn description(&self) -> &str {
        "lists the existing pipes"
    }

    type Plugin = PipePlugin;

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let pipe_ids = plugin
            .state()?
            .list_pipes()
            .map(|pipe_id| pipe_id.0.clone().into_value(Span::unknown()))
            .collect::<Vec<_>>();

        Value::list(pipe_ids, Span::unknown())
        .into_pipeline_data()
        .pipe(Ok)
    }
}
