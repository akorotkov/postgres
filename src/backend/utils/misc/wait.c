#include "postgres.h"

#include "utils/wait.h"

wait_event_start_hook_type	wait_event_start_hook;
wait_event_stop_hook_type	wait_event_stop_hook;
