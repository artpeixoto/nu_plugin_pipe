use crate::{PipeId, PipePlugin, PipeReaderValue, PipeWriterValue, PipeState, PipeValue};
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    FromValue, IntoValue, LabeledError, PipelineData, PipelineMetadata, Record, ShellError,
    Signature, Span, SyntaxShape, Type, Value,
    engine::{self, Call, Command, EngineState, Stack},
};

#[derive(Clone)]
pub struct GetPipeCmd;

impl PluginCommand for GetPipeCmd {
    fn name(&self) -> &str {
        "pipe get"
    }

    fn signature(&self) -> Signature {
        Signature::new(self.name())
            .required(
                "name",
                SyntaxShape::String,
                "the name of the pipe you want to get",
            )
            .input_output_type(Type::Nothing, PipeValue::expected_type())
        // .optional("new", SyntaxShape::Nothing, desc)
    }

    fn description(&self) -> &str {
        "Gets a pipe for you, if it exists"
    }

    type Plugin = PipePlugin;

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let name = {
            let name = call
                .positional
                .first()
                .ok_or_else(|| {
                    LabeledError::new("Missing name arg").with_label("this motherfucker", call.head)
                })?
                .clone();
            let name = name
                .into_string()
                .map_err(|e| LabeledError::new("name arg is not a string").with_inner(e))?;
            PipeId(name)
        };

        let state = plugin.state()?;

        let pipe_value = state.get_pipe_value(&name).ok_or_else(|| {
            LabeledError::new("pipe with that name either does not exist or has been closed")
        })?;

        let result = PipelineData::value(pipe_value.into_value(Span::unknown()), None);
        Ok(result)
    }
}

pub struct GetPipeSenderCmd ;

impl PluginCommand for GetPipeSenderCmd {
    fn name(&self) -> &str {
        "pipe get --sender"
    }

    fn signature(&self) -> Signature {
        Signature::new(self.name())
            .required(
                "name",
                SyntaxShape::String,
                "the name of the pipe whose sender you want to get",
            )
            .input_output_type(Type::Nothing, PipeWriterValue::expected_type())
        // .optional("new", SyntaxShape::Nothing, desc)
    }

    fn description(&self) -> &str {
        "Gets the sender to a pipe for you, if it exists."
    }

    type Plugin = PipePlugin;

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let name = {
            let name = call
                .positional
                .first()
                .ok_or_else(|| {
                    LabeledError::new("Missing name arg").with_label("this motherfucker", call.head)
                })?
                .clone();
            let name = name
                .into_string()
                .map_err(|e| LabeledError::new("name arg is not a string").with_inner(e))?;
            PipeId(name)
        };

        let state = plugin.state()?;

        let pipe_value = state
            .get_pipe_value(&name)
            .ok_or_else(|| {
                LabeledError::new("pipe with that name either does not exist or has been closed")
            })?
            .into_sender();

        let result = PipelineData::value(pipe_value.into_value(Span::unknown()), None);
        Ok(result)
    }
}

pub struct GetPipeReceiverCmd ;

impl PluginCommand for GetPipeReceiverCmd {
    fn name(&self) -> &str {
        "pipe get --receiver"
    }

    fn signature(&self) -> Signature {
        Signature::new(self.name())
            .required(
                "name",
                SyntaxShape::String,
                "the name of the pipe whose receiver you want to get",
            )
            .input_output_type(Type::Nothing, PipeReaderValue::expected_type())
        // .optional("new", SyntaxShape::Nothing, desc)
    }

    fn description(&self) -> &str {
        "Gets the receiver to a pipe for you, if it exists."
    }

    type Plugin = PipePlugin;

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let name = {
            let name = call
                .positional
                .first()
                .ok_or_else(|| {
                    LabeledError::new("Missing name arg").with_label("this motherfucker", call.head)
                })?
                .clone();

            let name = name
                .into_string()
                .map_err(|e|
	                LabeledError::new("name arg is not a string")
	                .with_inner(e)
                )?;

            PipeId(name)
        };

        let state = plugin.state()?;

        let pipe_value = state
            .get_pipe_value(&name)
            .ok_or_else(|| { LabeledError::new("pipe with that name either does not exist or has been closed") })?
            .into_receiver();

        let result = PipelineData::value(pipe_value.into_value(Span::unknown()), None);

        Ok(result)
    }
}
