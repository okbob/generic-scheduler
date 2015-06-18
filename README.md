# generic-scheduler

PostgreSQL generic SQL statements scheduler without additional dependencies (requires 9.5)

This architecture is based on concept one central scheduler process, that starts worker
process when it is necessary. This is less expensive than one controller process per database.

It can start and run more parallel same or different jobs. For example - I can start every
seconds 10 same jobs for processing one field from queue <=> queue will be processed by
parallel jobs and more CPU will be better saturated.

This scheduler is working only with stateless jobs - it is executed only
once or repeatedly or when some notify event is received. So this scheduler is not able to
do tasks like 'repeat 10 times when it fails, when the A fails, run the B, and similar. This
limit is by design - because this scheduler should be use simply for almost all simple tasks,
or can be used as base platform for development some complex schedulers. These complex schedulers
can be designed better in PL/pgSQL than in C.