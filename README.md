# luaproc---messaging-tables-async-read

Credit for original files: https://github.com/askyrme/luaproc

#Changes

Extended `luaproc.send` and `luaproc.receive` to add table support.
Extended the API to add IO and AIO.

## Table support

`luaproc.send` and `luaproc.receive` are now allowed to send Lua tables.
Though the contents of the table are restricted to the allowed types, that is:
boolean, nil, number, string or table values.
(only boolean, number or string values are permited as table keys)

## Extended API

**`luaproc.fileopen( string filename, string filemode )`**

Opens the given file in the selected mode and returns it's file descriptor, if successful.
Otherwise, returns nil and an error message.

**`luaproc.fileclose( int filedescriptor )`**

Closes the file given it's file descriptor (obtained with `luaproc.fileopen`).
Returns the value of `close()` (zero on success, -1 on error)

**`luaproc.read( int filedescriptor, int chunksizetoread )`**

Performs a synchronous file read and returns the chunk read, if successful.
Otherwise, returns nil and an error message.

**`luaproc.asyncread( int filedescriptor[, int chunksizetoread] )`**

Performs an asynchronous file read and returns the chunk read, if successful.
Otherwise, returns nil and an error message. If the chunk size isn't defined, default is the whole file.

**`luaproc.write( int filedescriptor, string contenttowrite )`**

Performs a synchronous file write of the given content and returns the file descriptor, if successful.
Otherwise, returns nil and an error message.

**`luaproc.asyncwrite( int filedescriptor, string contenttowrite )`**

Performs an asynchronous file write of the given content and returns the file descriptor, if successful.
Otherwise, returns nil and an error message.
