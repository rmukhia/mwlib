use std::marker::PhantomData;
use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::thread::JoinHandle;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MwLibError {
    #[error("io error: `{0}`")]
    IoError(#[from] std::io::Error),
    #[error("parse int error: `{0}`")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("named error: `{0}`")]
    NamedError(String),
    #[error("application error `{0}`")]
    AppError(#[from] Box<dyn std::error::Error>),
    #[error("unknown error")]
    Unknown,
}

pub type MwLibResult<T> = Result<T, MwLibError>;
pub type MwLibSender<T> = mpsc::Sender<T>;
pub type MwLibReceiver<T> = mpsc::Receiver<T>;
pub type MwLibUserDataWrapper<T> = Arc<RwLock<T>>;

pub type DispatchOperation<RxMessage, IntermediateMessage> =
    fn(IntermediateMessage, usize) -> MwLibResult<(usize, RxMessage)>;
pub type SubscribeOperation<RxMessage, IntermediateMessage, UserDataType> = fn(
    &RwLock<UserDataType>,
    &Vec<MwLibSender<RxMessage>>,
    usize,
    DispatchOperation<RxMessage, IntermediateMessage>,
);
pub type InitializeWorkerOperation<InitWorkerDataType, UserDataType> =
    fn(&RwLock<UserDataType>, usize) -> MwLibResult<InitWorkerDataType>;
pub type WorkerOperation<RxMessage, InitWorkerDataType, UserDataType> =
    fn(&RwLock<UserDataType>, &mut InitWorkerDataType, RxMessage, usize) -> MwLibResult<()>;

pub struct Client<RxMessage, IntermediateMessage, InitWorkerDataType, UserDataType> {
    user_data: MwLibUserDataWrapper<UserDataType>,
    message_placeholder: PhantomData<RxMessage>,
    intermediate_message_placeholder: PhantomData<IntermediateMessage>,
    init_worker_data_placeholder: PhantomData<InitWorkerDataType>,
}

pub struct Options<RxMessage, IntermediateMessage, InitWorkerDataType, UserDataType> {
    pub num_workers: usize,
    pub subscribe_operation: SubscribeOperation<RxMessage, IntermediateMessage, UserDataType>,
    pub dispatch_operation: DispatchOperation<RxMessage, IntermediateMessage>,
    pub worker_operation: WorkerOperation<RxMessage, InitWorkerDataType, UserDataType>,
    pub initialize_worker_operation: InitializeWorkerOperation<InitWorkerDataType, UserDataType>,
    pub thread_prefix: Option<String>,
    pub thread_stack_size: Option<usize>,
}

impl<
        RxMessage: Send + 'static,
        IntermediateMessage: 'static,
        InitWorkerDataType: 'static,
        UserDataType: Send + Sync + 'static,
    > Client<RxMessage, IntermediateMessage, InitWorkerDataType, UserDataType>
{
    pub fn new(
        user_data: MwLibUserDataWrapper<UserDataType>,
    ) -> MwLibResult<Client<RxMessage, IntermediateMessage, InitWorkerDataType, UserDataType>> {
        let client: Client<RxMessage, IntermediateMessage, InitWorkerDataType, UserDataType> =
            Client {
                user_data,
                message_placeholder: Default::default(),
                intermediate_message_placeholder: Default::default(),
                init_worker_data_placeholder: Default::default(),
            };

        return Ok(client);
    }

    fn mqtt_worker_thread(
        user_data: MwLibUserDataWrapper<UserDataType>,
        rx_queue: MwLibReceiver<RxMessage>,
        worker_id: usize,
        worker_operation: WorkerOperation<RxMessage, InitWorkerDataType, UserDataType>,
        init_worker_operation: InitializeWorkerOperation<InitWorkerDataType, UserDataType>,
    ) {
        let mut worker_data = match init_worker_operation(&user_data, worker_id) {
            Ok(val) => val,
            Err(err) => {
                eprintln!("[worker {worker_id}] initialization failed: {}", err);
                return;
            }
        };

        loop {
            // let the thread print errors
            let rx_msg = match rx_queue.recv() {
                Ok(value) => value,
                Err(err) => {
                    eprintln!("[worker {worker_id}] receive queue failure: {}", err);
                    continue;
                }
            };

            // let the thread print errors
            match worker_operation(user_data.as_ref(), &mut worker_data, rx_msg, worker_id) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("[worker {worker_id}] worker operation failure: {}", err);
                }
            };
        }
    }

    pub fn start_threads(
        &mut self,
        options: Options<RxMessage, IntermediateMessage, InitWorkerDataType, UserDataType>,
    ) -> MwLibResult<(Vec<JoinHandle<()>>, JoinHandle<()>)> {
        let mut tx_queues: Vec<MwLibSender<RxMessage>> = Vec::new();

        let mut worker_jh: Vec<JoinHandle<_>> = Vec::new();

        let thread_prefix = options.thread_prefix.unwrap_or(String::new());
        let stack_size = options.thread_stack_size.unwrap_or(1024 * 1024 * 2);

        for worker_id in 0..options.num_workers {
            let (tx, rx) = mpsc::channel();
            tx_queues.push(tx);

            let user_data = self.user_data.clone();
            let worker_handle = thread::Builder::new()
                .name(format!("{}worker-{}", thread_prefix, worker_id))
                .stack_size(stack_size)
                .spawn(move || {
                    Self::mqtt_worker_thread(
                        user_data,
                        rx,
                        worker_id,
                        options.worker_operation,
                        options.initialize_worker_operation,
                    );
                })?;

            worker_jh.push(worker_handle);
        }

        let mqtt_subscriber_thread = thread::Builder::new()
            .name(format!("{}mqtt_subscriber", thread_prefix))
            .stack_size(stack_size)
            .spawn({
                let user_data = self.user_data.clone();
                //let subscribe_operation = options.subscribe_operation.clone();
                //let dispatch_operation = options.dispatch_operation.clone();
                //let init_worker_operation = options.initialize_worker_operation.clone();
                move || {
                    (options.subscribe_operation)(
                        user_data.as_ref(),
                        &tx_queues,
                        options.num_workers,
                        options.dispatch_operation,
                    );
                }
            })?;

        Ok((worker_jh, mqtt_subscriber_thread))
    }
}
