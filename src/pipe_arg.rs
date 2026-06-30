use nu_protocol::{FromValue, LabeledError, Span, Type, Value};

use crate::{PipeId, PipeReaderValue, PipeValue};

pub fn pipe_arg_from_value(
    value: Value,
    allowed_types: &PipeArgAllowedTypes,
) -> Result<PipeId, LabeledError> {
    let span = value.span();
    if allowed_types.full
        && let Ok(val) = PipeValue::from_value(value.clone())
    {
        return Ok(val.0);
    }
    // if allowed_types.writer
    //     && let Ok(val) = PipeWriterValue::from_value(value.clone())
    // {
    //     return Ok(val.0);
    // }
    // if allowed_types.reader
    //     && let Ok(val) = PipeReaderValue::from_value(value.clone())
    // {
    //     return Ok(val.0);
    // }
    if allowed_types.string
        && let Ok(val) = String::from_value(value)
    {
        return Ok(PipeId(val));
    }

    return Err(
    	LabeledError::new("argument is not a valid pipe input")
     	.with_label("here", span)
    );
}

pub fn pipe_arg_type (
	allowed_types: &PipeArgAllowedTypes
)  -> Type{
	Type::one_of(
		[
			allowed_types.full.then_some(PipeValue::expected_type()),
	        // allowed_types.writer.then_some(PipeWriterValue::expected_type()),
	        // allowed_types.reader.then_some(PipeReaderValue::expected_type()),
	        allowed_types.string.then_some(Type::String),
        ]
        .into_iter()
        .flatten()
	)
}

pub struct PipeArgAllowedTypes {
    pub full: bool,
    // pub writer: bool,
    // pub reader: bool,
    pub string: bool,
}

impl PipeArgAllowedTypes {
    pub const fn all() -> Self {
        Self {
            full: true,
            // writer: true,
            // reader: true,
            string: true,
        }
    }

    #[inline(always)]
    pub const fn only_readers(mut self) -> Self {
        // self.writer = false;
        self
    }

    #[inline(always)]
    pub const fn only_writers(mut self) -> Self {
        // self.reader = false;
        self
    }

    #[inline(always)]
    pub const fn only_typed(mut self) -> Self {
        self.string = false;
        self
    }
}


impl PipeArgAllowedTypes{

	#[inline(always)]
	pub fn parse_value(&self, value: Value) ->  Result<PipeId, LabeledError> {
		pipe_arg_from_value(value, self)
	}

	#[inline(always)]
	pub fn arg_type(&self, ) -> Type{
		pipe_arg_type(self)
	}
}
