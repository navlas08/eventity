use crate::request::{Request, RequestHandler};
use std::any::{Any, TypeId};
use std::collections::HashMap;

/// Stores request handlers and event handlers
#[derive(Default)]
pub struct HandlerRegistry {
    request_handlers: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl HandlerRegistry {
    pub fn get_handler<R: Request>(&self) -> Option<&dyn RequestHandler<R>> {
        self.request_handlers
            .get(&TypeId::of::<R>())?
            .downcast_ref::<Box<dyn RequestHandler<R>>>()
            .map(|handler| handler.as_ref())
    }
    pub fn register<R, H>(&mut self, handler: H) -> bool
    where
        R: Request,
        H: RequestHandler<R>,
    {
        let trait_object: Box<dyn RequestHandler<R>> = Box::new(handler);

        self.request_handlers
            .insert(TypeId::of::<R>(), Box::new(trait_object))
            .is_none()
    }
}
