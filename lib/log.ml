let src = Logs.Src.create ~doc:"Main logging source for Castor." "castor"

include (val Logs.src_log src : Logs.LOG)
