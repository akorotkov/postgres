#include "postgres.h"

#include "utils/wait.h"

WaitEventStartHook	wait_event_start_hook;
WaitEventStopHook	wait_event_stop_hook;
