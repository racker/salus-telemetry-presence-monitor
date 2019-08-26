## Overview

The code in this module allows processes to add/update/delete tasks to a distributed workq and allows the workers in the q to be aware of each other and increase/reduce their share of the total work, as workers come and go.

The workq is implemented with a set of etcd kv's prefixed with the name "registry". Each new task in the q is a new kv pair with that prefix.

As workers pick up tasks from the q, they create an analogous entry in etcd with the prefix "active". That way the other workers know the task is in progress.

Finally, there is a set of kv pairs with the prefix "workers". This allows the workers to know of the existence of the others so they can shed work if the load becomes unbalanced.

This module manages the work/worker q's by creating three threads:

The registry thread watches for incoming work in the "registry" kv pairs and "grabs" it in a etcd transaction to ensure no two workers grab the same task.

The worker thread checks for incoming workers and sheds tasks when it determines it has too many.

Finally, the active thread checks for tasks shed by other processes and attempts to grab them if possible.
