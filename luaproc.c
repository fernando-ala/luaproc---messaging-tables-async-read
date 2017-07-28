/*
** luaproc API
** See Copyright Notice in luaproc.h
*/

#include <pthread.h>
#include <stdlib.h>
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

/*new*/
#include <unistd.h> /* close */
#include <fcntl.h> /* open, O_RDONLY */
#include <aio.h> /* aio_read, aio_error, aio_return */
#include <errno.h> /* EINPROGRESS */
#include <string.h> /* memset */
#include <sys/stat.h> /* asyncread - stat(filename, &st); */
/*end new*/

#include "luaproc.h"
#include "lpsched.h"

#define FALSE 0
#define TRUE  !FALSE
#define LUAPROC_CHANNELS_TABLE "channeltb"
#define LUAPROC_RECYCLE_MAX 0
/*new*/
#define AIO_READ_BUFSIZE 8192 //Default buffer size
/*end new*/

#if (LUA_VERSION_NUM == 501)

#define lua_pushglobaltable( L )    lua_pushvalue( L, LUA_GLOBALSINDEX )
#define luaL_newlib( L, funcs )     { lua_newtable( L ); \
  luaL_register( L, NULL, funcs ); }
#define isequal( L, a, b )          lua_equal( L, a, b )
#define requiref( L, modname, f, glob ) {\
  lua_pushcfunction( L, f ); /* push module load function */ \
  lua_pushstring( L, modname );  /* argument to module load function */ \
  lua_call( L, 1, 1 );  /* call 'f' to load module */ \
  /* register module in package.loaded table in case 'f' doesn't do so */ \
  lua_getfield( L, LUA_GLOBALSINDEX, LUA_LOADLIBNAME );\
  if ( lua_type( L, -1 ) == LUA_TTABLE ) {\
    lua_getfield( L, -1, "loaded" );\
    if ( lua_type( L, -1 ) == LUA_TTABLE ) {\
      lua_getfield( L, -1, modname );\
      if ( lua_type( L, -1 ) == LUA_TNIL ) {\
        lua_pushvalue( L, 1 );\
        lua_setfield( L, -3, modname );\
      }\
      lua_pop( L, 1 );\
    }\
    lua_pop( L, 1 );\
  }\
  lua_pop( L, 1 );\
  if ( glob ) { /* set global name? */ \
    lua_setglobal( L, modname );\
  } else {\
    lua_pop( L, 1 );\
  }\
}

#else

#define isequal( L, a, b )                 lua_compare( L, a, b, LUA_OPEQ )
#define requiref( L, modname, f, glob ) \
  { luaL_requiref( L, modname, f, glob ); lua_pop( L, 1 ); }

#endif

#if (LUA_VERSION_NUM >= 503)
#define dump( L, writer, data, strip )     lua_dump( L, writer, data, strip )
#define copynumber( Lto, Lfrom, i ) {\
  if ( lua_isinteger( Lfrom, i )) {\
    lua_pushinteger( Lto, lua_tonumber( Lfrom, i ));\
  } else {\
    lua_pushnumber( Lto, lua_tonumber( Lfrom, i ));\
  }\
}
#else
#define dump( L, writer, data, strip )     lua_dump( L, writer, data )
#define copynumber( Lto, Lfrom, i ) \
  lua_pushnumber( Lto, lua_tonumber( Lfrom, i ))
#endif

/********************
 * global variables *
 *******************/
 
/* channel list mutex */
static pthread_mutex_t mutex_channel_list = PTHREAD_MUTEX_INITIALIZER;

/* recycle list mutex */
static pthread_mutex_t mutex_recycle_list = PTHREAD_MUTEX_INITIALIZER;

/* recycled lua process list */
static list recycle_list;

/* maximum lua processes to recycle */
static int recyclemax = LUAPROC_RECYCLE_MAX;

/* lua_State used to store channel hash table */
static lua_State *chanls = NULL;

/* lua process used to wrap main state. allows main state to be queued in 
   channels when sending and receiving messages */
static luaproc mainlp;

/* main state matched a send/recv operation conditional variable */
pthread_cond_t cond_mainls_sendrecv = PTHREAD_COND_INITIALIZER;

/* main state communication mutex */
static pthread_mutex_t mutex_mainls = PTHREAD_MUTEX_INITIALIZER;

/***********************
 * register prototypes *
 ***********************/

static void luaproc_openlualibs( lua_State *L );
static int luaproc_create_newproc( lua_State *L );
static int luaproc_wait( lua_State *L );
static int luaproc_send( lua_State *L );
static int luaproc_receive( lua_State *L );
static int luaproc_create_channel( lua_State *L );
static int luaproc_destroy_channel( lua_State *L );
static int luaproc_set_numworkers( lua_State *L );
static int luaproc_get_numworkers( lua_State *L );
static int luaproc_recycle_set( lua_State *L );
LUALIB_API int luaopen_luaproc( lua_State *L );
static int luaproc_loadlib( lua_State *L ); 
/*new prototypes*/
static int luaproc_fileopen (lua_State *L);
static int luaproc_fileclose (lua_State *L);
static int luaproc_read (lua_State *L);
static int luaproc_async_read (lua_State *L);
static int luaproc_write (lua_State *L);
static int luaproc_async_write (lua_State *L);
static int asyncread_continuation (lua_State *L);
static int asyncwrite_continuation (lua_State *L);
static int pushLuaTableCopy( lua_State *Lfrom, lua_State *Lto, int tableIndex_Lfrom );
static int pushTableValueCopy( lua_State *Lfrom, lua_State *Lto );
static int pushTableKeyCopy( lua_State *Lfrom, lua_State *Lto );
/*end new*/

/***********
 * structs *
 ***********/

/*new*/
typedef struct staioreadstruct{
  long currentBufSize;
  char *aioReadBuffer;
} aioreadst;
/*end new*/

/* lua process */
struct stluaproc {
  lua_State *lstate;
  int status;
  int args;
  channel *chan;
  luaproc *next;
  /*new*/
  struct aiocb *ctrlBlck;
  aioreadst *aioReadStruct;
  /*end new*/ 
};

/* communication channel */
struct stchannel {
  list send;
  list recv;
  pthread_mutex_t mutex;
  pthread_cond_t can_be_used;
};

/* luaproc function registration array */
static const struct luaL_Reg luaproc_funcs[] = {
  { "newproc", luaproc_create_newproc },
  { "wait", luaproc_wait },
  { "send", luaproc_send },
  { "receive", luaproc_receive },
  { "newchannel", luaproc_create_channel },
  { "delchannel", luaproc_destroy_channel },
  { "setnumworkers", luaproc_set_numworkers },
  { "getnumworkers", luaproc_get_numworkers },
  { "recycle", luaproc_recycle_set },
  /*new*/
  { "fileopen", luaproc_fileopen },
  { "fileclose", luaproc_fileclose },
  { "read", luaproc_read},
  { "asyncread", luaproc_async_read},
  { "write", luaproc_write},
  { "asyncwrite", luaproc_async_write},
  /*end new*/
  { NULL, NULL }
};

/******************
 * list functions *
 ******************/

/* insert a lua process in a (fifo) list */
void list_insert( list *l, luaproc *lp ) {
  if ( l->head == NULL ) {
    l->head = lp;
  } else {
    l->tail->next = lp;
  }
  l->tail = lp;
  lp->next = NULL;
  l->nodes++;
}

/* remove and return the first lua process in a (fifo) list */
luaproc *list_remove( list *l ) {
  if ( l->head != NULL ) {
    luaproc *lp = l->head;
    l->head = lp->next;
    l->nodes--;
    return lp;
  } else {
    return NULL; /* if list is empty, return NULL */
  }
}

/* return a list's node count */
int list_count( list *l ) {
  return l->nodes;
}

/* initialize an empty list */
void list_init( list *l ) {
  l->head = NULL;
  l->tail = NULL;
  l->nodes = 0;
}

/*********************
 * channel functions *
 *********************/

/* create a new channel and insert it into channels table */
static channel *channel_create( const char *cname ) {

  channel *chan;

  /* get exclusive access to channels list */
  pthread_mutex_lock( &mutex_channel_list );

  /* create new channel and register its name */
  lua_getglobal( chanls, LUAPROC_CHANNELS_TABLE );
  chan = (channel *)lua_newuserdata( chanls, sizeof( channel ));
  lua_setfield( chanls, -2, cname );
  lua_pop( chanls, 1 );  /* remove channel table from stack */

  /* initialize channel struct */
  list_init( &chan->send );
  list_init( &chan->recv );
  pthread_mutex_init( &chan->mutex, NULL );
  pthread_cond_init( &chan->can_be_used, NULL );

  /* release exclusive access to channels list */
  pthread_mutex_unlock( &mutex_channel_list );

  return chan;
}

/*
   return a channel (if not found, return null).
   caller function MUST lock 'mutex_channel_list' before calling this function.
 */
static channel *channel_unlocked_get( const char *chname ) {

  channel *chan;

  lua_getglobal( chanls, LUAPROC_CHANNELS_TABLE );
  lua_getfield( chanls, -1, chname );
  chan = (channel *)lua_touserdata( chanls, -1 );
  lua_pop( chanls, 2 );  /* pop userdata and channel */

  return chan;
}

/*
   return a channel (if not found, return null) with its (mutex) lock set.
   caller function should unlock channel's (mutex) lock after calling this
   function.
 */
static channel *channel_locked_get( const char *chname ) {

  channel *chan;

  /* get exclusive access to channels list */
  pthread_mutex_lock( &mutex_channel_list );

  /*
     try to get channel and lock it; if lock fails, release external
     lock ('mutex_channel_list') to try again when signaled -- this avoids
     keeping the external lock busy for too long. during the release,
     the channel may be destroyed, so it must try to get it again.
  */
  while ((( chan = channel_unlocked_get( chname )) != NULL ) &&
        ( pthread_mutex_trylock( &chan->mutex ) != 0 )) {
    pthread_cond_wait( &chan->can_be_used, &mutex_channel_list );
  }

  /* release exclusive access to channels list */
  pthread_mutex_unlock( &mutex_channel_list );

  return chan;
}

/********************************
 * exported auxiliary functions *
 ********************************/

/* unlock access to a channel and signal it can be used */
void luaproc_unlock_channel( channel *chan ) {

  /* get exclusive access to channels list */
  pthread_mutex_lock( &mutex_channel_list );
  /* release exclusive access to operate on a particular channel */
  pthread_mutex_unlock( &chan->mutex );
  /* signal that a particular channel can be used */
  pthread_cond_signal( &chan->can_be_used );
  /* release exclusive access to channels list */
  pthread_mutex_unlock( &mutex_channel_list );

}

/* insert lua process in recycle list */
void luaproc_recycle_insert( luaproc *lp ) {

  /* get exclusive access to recycled lua processes list */
  pthread_mutex_lock( &mutex_recycle_list );

  /* is recycle list full? */
  if ( list_count( &recycle_list ) >= recyclemax ) {
    /* destroy state */
    lua_close( luaproc_get_state( lp ));
  } else {
    /* insert lua process in recycle list */
    list_insert( &recycle_list, lp );
  }

  /* release exclusive access to recycled lua processes list */
  pthread_mutex_unlock( &mutex_recycle_list );
}

/* queue a lua process that tried to send a message */
void luaproc_queue_sender( luaproc *lp ) {
  list_insert( &lp->chan->send, lp );
}

/* queue a lua process that tried to receive a message */
void luaproc_queue_receiver( luaproc *lp ) {
  list_insert( &lp->chan->recv, lp );
}

/*new*/
void luaproc_queue_aio( list *blocked_aio_list, luaproc *lp ) {
  list_insert( blocked_aio_list, lp );
}

struct aiocb* luaproc_get_aio_ctrlblock( luaproc *lp ){
  return lp->ctrlBlck;
}

struct aiocb * create_aioctrlBlck( ){
	struct aiocb *ctrlBlck = (struct aiocb *)malloc(sizeof(struct aiocb));
	memset(ctrlBlck, 0, sizeof(struct aiocb));//Set everything as 0
	return ctrlBlck;
}

char * create_aioReadBuffer( ){
	char *buf = (char *)malloc(AIO_READ_BUFSIZE * sizeof(char));//TODO:ver qual é o melhor tamanho default ( 8192? )
	return buf;
}

int realloc_aioReadBuffer( char** buf, long size ){
	*buf = (char *)realloc(*buf, size * sizeof(char));
	if( *buf ==NULL )
		return 0;
	return 1;
}

aioreadst * create_aioReadStruct( ){
	aioreadst *aioSt = (aioreadst *)malloc(sizeof(aioreadst *));
	aioSt->currentBufSize = AIO_READ_BUFSIZE;
	aioSt->aioReadBuffer = create_aioReadBuffer();	
	return aioSt;
}
/*end new*/

/********************************
 * internal auxiliary functions *
 ********************************/
static void luaproc_loadbuffer( lua_State *parent, luaproc *lp,
                                const char *code, size_t len ) {

  /* load lua process' lua code */
  int ret = luaL_loadbuffer( lp->lstate, code, len, code );

  /* in case of errors, close lua_State and push error to parent */
  if ( ret != 0 ) {
    lua_pushstring( parent, lua_tostring( lp->lstate, -1 ));
    lua_close( lp->lstate );
    luaL_error( parent, lua_tostring( parent, -1 ));
  }
}

/* copies values between lua states' stacks */
static int luaproc_copyvalues( lua_State *Lfrom, lua_State *Lto ) {

  int i;
  int n = lua_gettop( Lfrom );
  const char *str;
  size_t len;
  
  /* ensure there is space in the receiver's stack */
  if ( lua_checkstack( Lto, n ) == 0 ) {
    lua_pushnil( Lto );
    lua_pushstring( Lto, "not enough space in the stack" );
    lua_pushnil( Lfrom );
    lua_pushstring( Lfrom, "not enough space in the receiver's stack" );
    return FALSE;
  }

  /* test each value's type and, if it's supported, copy value */
  for ( i = 2; i <= n; i++ ) {
    switch ( lua_type( Lfrom, i )) {
      case LUA_TBOOLEAN:
        lua_pushboolean( Lto, lua_toboolean( Lfrom, i ));
        break;
      case LUA_TNUMBER:
        copynumber( Lto, Lfrom, i );
        break;
      case LUA_TSTRING: {
        str = lua_tolstring( Lfrom, i, &len );
        lua_pushlstring( Lto, str, len );
        break;
      }
      case LUA_TNIL:
        lua_pushnil( Lto );
        break;
		/*new*/	
      case LUA_TTABLE:{			     
		 if( !pushLuaTableCopy(Lfrom, Lto, i) )
			return FALSE;
		 break;}
		 /*end new*/
      default: /* value type not supported: table, function, userdata, etc. */
        lua_settop( Lto, 1 );
        lua_pushnil( Lto );
        lua_pushfstring( Lto, "failed to receive value of unsupported type "
                                "'%s'", luaL_typename( Lfrom, i ));
        lua_pushnil( Lfrom );
        lua_pushfstring( Lfrom, "failed to send value of unsupported type "
                                "'%s'", luaL_typename( Lfrom, i ));
        return FALSE;
    }
  }
  return TRUE;
}

/*new*/
/*********************************
 * auxiliary lua table functions *
 *********************************/

 static int pushTableKeyCopy( lua_State *Lfrom, lua_State *Lto )
 {
	/* 'key' at index -2 */
	const char *str;
	size_t len;

 	switch ( lua_type( Lfrom, -2 )) {
	     case LUA_TBOOLEAN:
		 lua_pushboolean( Lto, lua_toboolean( Lfrom, -2 ));
		 break;
	     case LUA_TNUMBER:
		 lua_pushnumber(Lto, lua_tonumber( Lfrom, -2 ));
		 break;
	     case LUA_TSTRING:{
		 str = lua_tolstring( Lfrom, -2, &len );
		 lua_pushlstring( Lto, str, len );
		 break;
		 }
	     default: /* unsupported key type */
		 lua_settop( Lto, 1 );
		 lua_pushnil( Lto );
		 lua_pushfstring( Lto, "failed to receive unsupported table key type "
		                        "'%s'", luaL_typename( Lfrom, -2 ));
		 lua_pushnil( Lfrom );
		 lua_pushfstring( Lfrom, "failed to send unsupported table key type "
		                        "'%s'", luaL_typename( Lfrom, -2 ));
		 return FALSE;
	}
	return TRUE;
 } 

 static int pushTableValueCopy( lua_State *Lfrom, lua_State *Lto )
 {
	/* 'value' at index -1 */
	const char *str;
	size_t len;

 	switch ( lua_type( Lfrom, -1 )) {
	     case LUA_TBOOLEAN:
		 lua_pushboolean( Lto, lua_toboolean( Lfrom, -1 ));
		 break;
	     case LUA_TNUMBER:
		 lua_pushnumber(Lto, lua_tonumber( Lfrom, -1 ));
		 break;
	     case LUA_TSTRING:{
		 str = lua_tolstring( Lfrom, -1, &len );
		 lua_pushlstring( Lto, str, len );
		 break;
		 }
           case LUA_TNIL:
		 lua_pushnil( Lto );
 	 	 break;
 	   case LUA_TTABLE:{			     
		 if( !pushLuaTableCopy(Lfrom, Lto, (int)lua_gettop(Lfrom)/*get index of 'value'(table) in Lfrom Stack*/) )//Index is on top due to lua_next function
			return FALSE;
		 break;}
	     default: /* unsupported key type */
		 lua_settop( Lto, 1 );
		 lua_pushnil( Lto );
		 lua_pushfstring( Lto, "failed to receive unsupported table value type "
		                        "'%s'", luaL_typename( Lfrom, -1 ));
		 lua_pushnil( Lfrom );
		 lua_pushfstring( Lfrom, "failed to send unsupported table value type "
		                        "'%s'", luaL_typename( Lfrom, -1 ));
		 return FALSE;
	}
	return TRUE;
 } 

 static int pushLuaTableCopy( lua_State *Lfrom, lua_State *Lto, int tableIndex_Lfrom )
 {
	int top_Lto;

	/* ensure there is space in the receiver's stack (table, key and value for 'lua_settable') */
	  if ( lua_checkstack( Lto, 3 ) == 0 ) {
	    lua_pushnil( Lto );
	    lua_pushstring( Lto, "not enough space in my stack" );
	    lua_pushnil( Lfrom );
	    lua_pushstring( Lfrom, "not enough space in the receiver's stack" );
	    return FALSE;
	  }

	/* ensure there is space in the sender's stack (nil, key and value for 'lua_next') */
	  if ( lua_checkstack( Lfrom, 3 ) == 0 ) {
	    lua_pushnil( Lto );
	    lua_pushstring( Lto, "not enough space in sender's the stack" );
	    lua_pushnil( Lfrom );
	    lua_pushstring( Lfrom, "not enough space in my stack" );
	    return FALSE;
	  }	

	lua_pushnil(Lfrom);  /* first key for 'value'(table) iteration*/
	lua_newtable(Lto); /*creates empty table in Lto Stack*/
	top_Lto = lua_gettop(Lto);/*get top index of Lto Stack*/

	while (lua_next(Lfrom, tableIndex_Lfrom) != 0)/*while 'value'(table at top of Lfrom stack) still has elements*/ 
	{
		/*push 'key' then 'value' for the 'lua_settable' function*/
		/* 'key' at index -2 and 'value' at index -1 */
	
		/*Push Key*/
		if( !pushTableKeyCopy(Lfrom,Lto) )//if unsucessful
			return FALSE;

		/*Push Value (Table)*/
		if( !pushTableValueCopy(Lfrom,Lto) )//if unsucessful
			return FALSE;		

		/* removes 'value'; keeps 'key' for next iteration */
		lua_pop(Lfrom, 1);
		
	  	lua_settable(Lto, top_Lto);
	}
	
	return TRUE;
 }
 /*end new*/

/* return the lua process associated with a given lua state */
static luaproc *luaproc_getself( lua_State *L ) {

  luaproc *lp;

  lua_getfield( L, LUA_REGISTRYINDEX, "LUAPROC_LP_UDATA" );
  lp = (luaproc *)lua_touserdata( L, -1 );
  lua_pop( L, 1 );

  return lp;
}

/* create new lua process */
static luaproc *luaproc_new( lua_State *L ) {

  luaproc *lp;
  lua_State *lpst = luaL_newstate();  /* create new lua state */

  /* store the lua process in its own lua state */
  lp = (luaproc *)lua_newuserdata( lpst, sizeof( struct stluaproc ));
  lua_setfield( lpst, LUA_REGISTRYINDEX, "LUAPROC_LP_UDATA" );
  luaproc_openlualibs( lpst );  /* load standard libraries and luaproc */
  /* register luaproc's own functions */
  requiref( lpst, "luaproc", luaproc_loadlib, TRUE );
  lp->lstate = lpst;  /* insert created lua state into lua process struct */
  /*new*/
  lp->ctrlBlck = NULL;
  lp->aioReadStruct = NULL;
  /*end new*/
  
  return lp;
}

/* join schedule workers (called before exiting Lua) */
static int luaproc_join_workers( lua_State *L ) {
  sched_join_workers();
  lua_close( chanls );
  return 0;
}

/* writer function for lua_dump */
static int luaproc_buff_writer( lua_State *L, const void *buff, size_t size, 
                                void *ud ) {
  (void)L;
  luaL_addlstring((luaL_Buffer *)ud, (const char *)buff, size );
  return 0;
}

/* copies upvalues between lua states' stacks */
static int luaproc_copyupvalues( lua_State *Lfrom, lua_State *Lto, 
                                 int funcindex ) {

  int i = 1;
  const char *str;
  size_t len;

  /* test the type of each upvalue and, if it's supported, copy it */
  while ( lua_getupvalue( Lfrom, funcindex, i ) != NULL ) {
    switch ( lua_type( Lfrom, -1 )) {
      case LUA_TBOOLEAN:
        lua_pushboolean( Lto, lua_toboolean( Lfrom, -1 ));
        break;
      case LUA_TNUMBER:
        copynumber( Lto, Lfrom, -1 );
        break;
      case LUA_TSTRING: {
        str = lua_tolstring( Lfrom, -1, &len );
        lua_pushlstring( Lto, str, len );
        break;
      }
      case LUA_TNIL:
        lua_pushnil( Lto );
        break;
      /* if upvalue is a table, check whether it is the global environment
         (_ENV) from the source state Lfrom. in case so, push in the stack of
         the destination state Lto its own global environment to be set as the
         corresponding upvalue; otherwise, treat it as a regular non-supported
         upvalue type. */
      case LUA_TTABLE:
        lua_pushglobaltable( Lfrom );
        if ( isequal( Lfrom, -1, -2 )) {
          lua_pop( Lfrom, 1 );
          lua_pushglobaltable( Lto );
          break;
        }
        lua_pop( Lfrom, 1 );
        /* FALLTHROUGH */
      default: /* value type not supported: table, function, userdata, etc. */
        lua_pushnil( Lfrom );
        lua_pushfstring( Lfrom, "failed to copy upvalue of unsupported type "
                                "'%s'", luaL_typename( Lfrom, -2 ));
        return FALSE;
    }
    lua_pop( Lfrom, 1 );
    if ( lua_setupvalue( Lto, 1, i ) == NULL ) {
      lua_pushnil( Lfrom );
      lua_pushstring( Lfrom, "failed to set upvalue" );
      return FALSE;
    }
    i++;
  }
  return TRUE;
}


/*********************
 * library functions *
 *********************/

/* set maximum number of lua processes in the recycle list */
static int luaproc_recycle_set( lua_State *L ) {

  luaproc *lp;

  /* validate parameter is a non negative number */
  lua_Integer max = luaL_checkinteger( L, 1 );
  luaL_argcheck( L, max >= 0, 1, "recycle limit must be positive" );

  /* get exclusive access to recycled lua processes list */
  pthread_mutex_lock( &mutex_recycle_list );

  recyclemax = max;  /* set maximum number */

  /* remove extra nodes and destroy each lua processes */
  while ( list_count( &recycle_list ) > recyclemax ) {
    lp = list_remove( &recycle_list );
    lua_close( lp->lstate );
  }
  /* release exclusive access to recycled lua processes list */
  pthread_mutex_unlock( &mutex_recycle_list );

  return 0;
}

/* wait until there are no more active lua processes */
static int luaproc_wait( lua_State *L ) {
  sched_wait();
  return 0;
}

/* set number of workers (creates or destroys accordingly) */
static int luaproc_set_numworkers( lua_State *L ) {

  /* validate parameter is a positive number */
  lua_Integer numworkers = luaL_checkinteger( L, -1 );
  luaL_argcheck( L, numworkers > 0, 1, "number of workers must be positive" );

  /* set number of threads; signal error on failure */
  if ( sched_set_numworkers( numworkers ) == LUAPROC_SCHED_PTHREAD_ERROR ) {
      luaL_error( L, "failed to create worker" );
  } 

  return 0;
}

/* return the number of active workers */
static int luaproc_get_numworkers( lua_State *L ) {
  lua_pushnumber( L, sched_get_numworkers( ));
  return 1;
}

/* create and schedule a new lua process */
static int luaproc_create_newproc( lua_State *L ) {

  size_t len;
  luaproc *lp;
  luaL_Buffer buff;
  const char *code;
  int d;
  int lt = lua_type( L, 1 );

  /* check function argument type - must be function or string; in case it is
     a function, dump it into a binary string */
  if ( lt == LUA_TFUNCTION ) {
    lua_settop( L, 1 );
    luaL_buffinit( L, &buff );
    d = dump( L, luaproc_buff_writer, &buff, FALSE );
    if ( d != 0 ) {
      lua_pushnil( L );
      lua_pushfstring( L, "error %d dumping function to binary string", d );
      return 2;
    }
    luaL_pushresult( &buff );
    lua_insert( L, 1 );
  } else if ( lt != LUA_TSTRING ) {
    lua_pushnil( L );
    lua_pushfstring( L, "cannot use '%s' to create a new process",
                     luaL_typename( L, 1 ));
    return 2;
  }

  /* get pointer to code string */
  code = lua_tolstring( L, 1, &len );

  /* get exclusive access to recycled lua processes list */
  pthread_mutex_lock( &mutex_recycle_list );

  /* check if a lua process can be recycled */
  if ( recyclemax > 0 ) {
    lp = list_remove( &recycle_list );
    /* otherwise create a new lua process */
    if ( lp == NULL ) {
      lp = luaproc_new( L );
    }
  } else {
    lp = luaproc_new( L );
  }

  /* release exclusive access to recycled lua processes list */
  pthread_mutex_unlock( &mutex_recycle_list );

  /* init lua process */
  lp->status = LUAPROC_STATUS_IDLE;
  lp->args   = 0;
  lp->chan   = NULL;

  /* load code in lua process */
  luaproc_loadbuffer( L, lp, code, len );

  /* if lua process is being created from a function, copy its upvalues and
     remove dumped binary string from stack */
  if ( lt == LUA_TFUNCTION ) {
    if ( luaproc_copyupvalues( L, lp->lstate, 2 ) == FALSE ) {
      luaproc_recycle_insert( lp ); 
      return 2;
    }
    lua_pop( L, 1 );
  }

  sched_inc_lpcount();   /* increase active lua process count */
  sched_queue_proc( lp );  /* schedule lua process for execution */
  lua_pushboolean( L, TRUE );

  return 1;
}

/*new*/
/* Opening and returning a file descriptor */
static int luaproc_fileopen (lua_State *L)
{
	const char *fileName;
	size_t len;
	int file;
	const char *fileMode;
	
	/*test*/
	//firstTime = 1;
	/*end test*/
	
	if( lua_type( L, 1 ) == LUA_TSTRING) {
		fileName = lua_tolstring( L, 1, &len );//Arquive name in Lua Stack
	}
	else{
		lua_pushnil( L );
        lua_pushstring( L, "Invalid file name/path." );
        return 2;
	}
	if( lua_type( L, 2 ) == LUA_TSTRING) {
		fileMode = lua_tolstring( L, 2, &len );//Arquive open mode in Lua Stack
	}
	else{
		lua_pushnil( L );
        lua_pushstring( L, "Invalid file mode" );
        return 2;
	}
		
	if ( !strcmp(fileMode, "r") )//"r" 	Read-only mode and is the default mode where an existing file is opened.
        file = open(fileName, O_RDONLY);
    else if ( !strcmp(fileMode, "w") )//"w" 	Write enabled mode that overwrites the existing file or creates a new file.
        file = open(fileName, O_CREAT | O_WRONLY, 0777);//S_IRWXU | S_IRWXG | S_IRWXO
    else if ( !strcmp(fileMode, "a") )//"a" 	Append mode that opens an existing file or creates a new file for appending.
	    file = open(fileName, O_CREAT | O_WRONLY | O_APPEND, 0777);
    else if ( !strcmp(fileMode, "r+") )//"r+" 	Read and write mode for an existing file.
        file = open(fileName, O_RDWR);
    else if ( !strcmp(fileMode, "w+") )//"w+" 	All existing data is removed if file exists or new file is created with read write permissions.
        file = open(fileName, O_CREAT | O_RDWR | O_TRUNC, 0777);
    else if ( !strcmp(fileMode, "a+") )//"a+" 	Append mode with read mode enabled that opens an existing file or creates a new file.
        file = open(fileName, O_CREAT | O_APPEND | O_RDWR, 0777);
    else{ /* value type not supported. */
		lua_pushnil( L );
        lua_pushstring( L, "Unsuported file mode" );
        return 2;		
    }
	lua_pushnumber(L, file);
	return 1;
}

/* Closing a file descriptor */
static int luaproc_fileclose (lua_State *L)
{
	int file;
	int ret;
	
	if( lua_type( L, -1 ) == LUA_TNUMBER) {
		file = lua_tonumber( L, -1 );//File descriptor in Lua Stack
	}
	else
	{
		lua_pushstring( L, "Invalid file descriptor." );
		return 1;
	}
	
	ret =  close(file);
	lua_pushnumber( L, ret );
	return 1;
}

/* syncronous file read, without buffering (using 'read' not 'fread') */
static int luaproc_read (lua_State *L)
{
	int file;
	long size;
	char* buf;
	long offset, sizeOfFile;
	int result;
	struct stat st;
	luaproc *self;	

	if( lua_type( L, 1 ) == LUA_TNUMBER) {
		file = lua_tonumber( L, 1 );//File descriptor in Lua Stack
	}
	else
	{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid file descriptor type." );
		return 2;
	}
	
	if( fcntl(file, F_GETFD) == -1 || errno == EBADF )
	{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid file descriptor value." );
		return 2;
	}

	if( lua_type( L, 2 ) == LUA_TNUMBER) {
		size = lua_tonumber( L, 2 );//File chunk size to read
	}
	else{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid size (in bytes) type for read." );
		return 2;
	}

	if (fstat(file, &st) == 0){ //if fstat successful
			sizeOfFile = st.st_size;//Read whole file
		}
	else{
		lua_pushnil( L );
		lua_pushstring( L, "Cannot determine file size." );
		return 2;
	}

	offset = lseek(file, 0, SEEK_CUR);		

	if(offset + size > sizeOfFile)//over the file limits
	{	
		size = sizeOfFile - offset;
	}

	self = luaproc_getself( L );
	if ( self != NULL )
	{
		if( self->aioReadStruct == NULL )//first time reading, allocate aio read struct
			self->aioReadStruct = create_aioReadStruct();//TODO:tratar os casos em que o buffer for menor que o chunk

		if( size > self->aioReadStruct->currentBufSize ){//chunk maior que o tamanho atual...tentar allocar o tamanho pedido
			if( !realloc_aioReadBuffer( &self->aioReadStruct->aioReadBuffer, size ) )
			{
				lua_pushnil( L );
				lua_pushstring( L, "Could not allocate chunk size for read." );
				return 2;
			}
			self->aioReadStruct->currentBufSize = size;
		}

		buf = self->aioReadStruct->aioReadBuffer;

		result = read( file, buf, size );
	
		if( result != -1 ){ //sucess
			if(result == 0) //eof
				lua_pushnil( L );
			else
				lua_pushlstring( L, buf, size );

			return 1;
		}
		else{
			lua_pushnil( L );
			lua_pushstring( L, "Read unsuccesful." );
			return 2;
		}
	}
	lua_pushnil( L );
	lua_pushstring( L, "Unable to acquire current lua state" );
	return 2;
}

/* Non-blocking reading */
static int luaproc_async_read (lua_State *L)
{	
	int file;
	luaproc *self;	
	long sizeOfFile, chunkSize;	
	long offset;
	char* inputBuffer;
	
	// Async IO control block structure
	struct aiocb *ctrlBlck;	
	struct stat st;
		
	if( lua_type( L, 1 ) == LUA_TNUMBER) {
		file = lua_tonumber( L, 1 );//File descriptor in Lua Stack
	}
	else
	{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid file descriptor type." );
		return 2;
	}
	
	if( fcntl(file, F_GETFD) == -1 || errno == EBADF )
	{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid file descriptor value." );
		return 2;
	}
	
	if (fstat(file, &st) == 0){ //if fstat successful
			sizeOfFile = st.st_size;//Read whole file
		}
	else{
		lua_pushnil( L );
		lua_pushstring( L, "Cannot determine file size." );
		return 2;
	}
	
	if( lua_type( L, 2 ) == LUA_TNUMBER) {
		chunkSize = lua_tonumber( L, 2 );//File chunk size to read
	}
	else{
		chunkSize = sizeOfFile;//Read whole file
	}		

	offset = lseek(file, 0, SEEK_CUR);	
	
	if(offset >= sizeOfFile)//over the file limits
	{	
		lua_pushnil( L );
		return 1;
	}
		
	if(offset + chunkSize > sizeOfFile)//over the file limits
	{	
		chunkSize = sizeOfFile - offset;
	}

	self = luaproc_getself( L );
	if ( self != NULL )
	{	
		if( self->ctrlBlck == NULL )//first time AIO, allocate aio struct and memset
			self->ctrlBlck = create_aioctrlBlck( );
		
		if( self->aioReadStruct == NULL )//first time reading, allocate aio read struct
			self->aioReadStruct = create_aioReadStruct();//TODO:tratar os casos em que o buffer for menor que o chunk

		if( chunkSize > self->aioReadStruct->currentBufSize ){//chunk maior que o tamanho atual...tentar allocar o tamanho pedido
			if( !realloc_aioReadBuffer( &self->aioReadStruct->aioReadBuffer, chunkSize ) )
			{
				lua_pushnil( L );
				lua_pushstring( L, "Could not allocate chunk size for read." );
				return 2;
			}
			self->aioReadStruct->currentBufSize = chunkSize;
		}

		inputBuffer = self->aioReadStruct->aioReadBuffer;
		ctrlBlck = self->ctrlBlck;

		// create the control block structure		
		ctrlBlck->aio_nbytes = chunkSize; //Length of transfer = Size to Read (size of buffer)
		ctrlBlck->aio_fildes = file;      //File descriptor
		ctrlBlck->aio_offset = offset;	  //File offset
		ctrlBlck->aio_buf = inputBuffer;  //Location of Buffer
		/*
		ctrlBlck.aio_reqprio    // Request priority
		ctrlBlck.aio_sigevent   // Notification method
		ctrlBlck.aio_lio_opcode // Operation to be performed; lio_listio() only
		*/

		/*Test if IO request was successful*/
		if (aio_read(ctrlBlck) == -1)
		{			
			lua_pushnil( L );
			lua_pushstring( L, "Unable to create async read request" );		
			return 2;
		}		

		return asyncread_continuation( L );
	}
	lua_pushnil( L );
	lua_pushstring( L, "Unable to acquire current lua state" );
	return 2;
}

/* continuation function for yielding when I/O not finished */
static int asyncread_continuation (lua_State *L)
{
	int numBytesRead;
	long offset;
	luaproc *self;
	self = luaproc_getself( L );	

	/*Test if IO is in progress*/
	if(aio_error(self->ctrlBlck) == EINPROGRESS)
	{
		/* yield */
		self->status = LUAPROC_STATUS_BLOCKED_AIO;
		return lua_yieldk( L, 0, 0, &asyncread_continuation);			
	}
	else//IO complete
	{	
		/* Test if successful */
		numBytesRead = aio_return(self->ctrlBlck);
		
		if (numBytesRead != -1)
		{
			offset = lseek(self->ctrlBlck->aio_fildes, 0, SEEK_CUR);
			offset += (long)self->ctrlBlck->aio_nbytes;
			lseek(self->ctrlBlck->aio_fildes, offset, SEEK_SET);//Set file offset (because aio_read doesn't for some reason)
			lua_pushlstring( L, (char *)self->ctrlBlck->aio_buf, (size_t)self->ctrlBlck->aio_nbytes );			
			return 1;
		}
		else
		{
			lua_pushnil( L );
			lua_pushstring( L, "Error! File read unsuccessful\n" );		
			return 2;
		}
	}
}

/* syncronous file write, without buffering (using 'write' not 'fwrite') */
static int luaproc_write (lua_State *L)
{
	const char *fileContentToWrite;
	size_t len;
	int file;
	int result;

	if( lua_type( L, 1 ) == LUA_TNUMBER) {
		file = lua_tonumber( L, 1 );//File descriptor in Lua Stack
	}
	else
	{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid file descriptor type." );
		return 2;
	}
	
	if( lua_type( L, 2 ) == LUA_TSTRING) {
		fileContentToWrite = lua_tolstring( L, 2, &len );//Arquive content to Write in Lua Stack
	}
	else
	{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid content to write" );
		return 2;
	}
	
	if( fcntl(file, F_GETFD) == -1 || errno == EBADF )
	{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid file descriptor value." );
		return 2;
	}

	result = write( file, fileContentToWrite, len );

	if( result != -1 ){ //sucess
		if(result == 0){ //eof
			lua_pushnil( L );
			lua_pushstring( L, "Nothing was writen." );
			return 2;
		}		
		
		lua_pushnumber( L, file );
		return 1;
	}
	else{
		lua_pushnil( L );
		lua_pushstring( L, "Error! File write unsuccesful." );
		return 2;
	}

}

/* Non-blocking writing */
static int luaproc_async_write (lua_State *L)
{		
	const char *fileContentToWrite;
	size_t len;
	long offset;
	int file;
	luaproc *self;
	// Async IO control block structure
	struct aiocb *ctrlBlck;
		
	if( lua_type( L, 1 ) == LUA_TNUMBER) {
		file = lua_tonumber( L, 1 );//File descriptor in Lua Stack
	}
	else
	{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid file descriptor type." );
		return 2;
	}
	
	if( lua_type( L, 2 ) == LUA_TSTRING) {
		fileContentToWrite = lua_tolstring( L, 2, &len );//Arquive content to Write in Lua Stack
	}
	else
	{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid content to write" );
		return 2;
	}
	
	if( fcntl(file, F_GETFD) == -1 || errno == EBADF )
	{
		lua_pushnil( L );
		lua_pushstring( L, "Invalid file descriptor value." );
		return 2;
	}
	
	offset = lseek(file, 0, SEEK_CUR);

	self = luaproc_getself( L );
	if ( self != NULL )
	{			
		if( self->ctrlBlck == NULL )//first time AIO, allocate aio struct and memset
			self->ctrlBlck = create_aioctrlBlck( );
		
		ctrlBlck = self->ctrlBlck;
		// create the control block structure
		ctrlBlck->aio_nbytes = len;			 	//Length of transfer = Size to Read (size of buffer)?
		ctrlBlck->aio_fildes = file;    			//File descriptor
		ctrlBlck->aio_offset = offset;	   			//File offset
		ctrlBlck->aio_buf = (char *)fileContentToWrite;	   	//Location of Buffer
		/*
		ctrlBlck.aio_reqprio    // Request priority
		ctrlBlck.aio_sigevent   // Notification method
		ctrlBlck.aio_lio_opcode // Operation to be performed; lio_listio() only
		*/
					
		/*Test if IO request was successful*/
		if (aio_write(ctrlBlck) == -1)
		{
			lua_pushnil( L );
			lua_pushstring( L, "Unable to create async write request" );	
			return 2;
		}			
	
		return asyncwrite_continuation( L );
	}
	lua_pushnil( L );
	lua_pushstring( L, "Unable to acquire current lua state" );
	return 2;
}

/* continuation function for yielding when I/O not finished */
static int asyncwrite_continuation (lua_State *L)
{
	luaproc *self;
	long offset;

	self = luaproc_getself( L );
	/*Test if IO is in progress*/
	if(aio_error(self->ctrlBlck) == EINPROGRESS)
	{
		/* yield */
		self->status = LUAPROC_STATUS_BLOCKED_AIO;
		return lua_yieldk( L, 0, 0, &asyncwrite_continuation);			
	}
	else//IO complete
	{	
		/* Test if unsuccessful */
		if(aio_error(self->ctrlBlck) > 0)
		{
			lua_pushnil( L );
			lua_pushstring( L, "Error! File write unsuccessful" );			
			return 2;
		}
		else
		{	
			offset = lseek(self->ctrlBlck->aio_fildes, 0, SEEK_CUR);
			offset += (long)self->ctrlBlck->aio_nbytes;		
			lseek(self->ctrlBlck->aio_fildes, offset, SEEK_SET);//Set file offset (because aio_write doesn't for some reason)
			lua_pushnumber( L, self->ctrlBlck->aio_fildes );
			return 1;		
		}
	}
}
/*end new*/

/* send a message to a lua process */
static int luaproc_send( lua_State *L ) {

  int ret;
  channel *chan;
  luaproc *dstlp, *self;
  const char *chname = luaL_checkstring( L, 1 );

  chan = channel_locked_get( chname );
  /* if channel is not found, return an error to lua */
  if ( chan == NULL ) {
    lua_pushnil( L );
    lua_pushfstring( L, "channel '%s' does not exist", chname );
    return 2;
  }

  /* remove first lua process, if any, from channel's receive list */
  dstlp = list_remove( &chan->recv );
  
  if ( dstlp != NULL ) { /* found a receiver? */
    /* try to move values between lua states' stacks */
    ret = luaproc_copyvalues( L, dstlp->lstate );
    /* -1 because channel name is on the stack */
    dstlp->args = lua_gettop( dstlp->lstate ) - 1; 
    if ( dstlp->lstate == mainlp.lstate ) {
      /* if sending process is the parent (main) Lua state, unblock it */
      pthread_mutex_lock( &mutex_mainls );
      pthread_cond_signal( &cond_mainls_sendrecv );
      pthread_mutex_unlock( &mutex_mainls );
    } else {
      /* schedule receiving lua process for execution */
      sched_queue_proc( dstlp );
    }
    /* unlock channel access */
    luaproc_unlock_channel( chan );
    if ( ret == TRUE ) { /* was send successful? */
      lua_pushboolean( L, TRUE );
      return 1;
    } else { /* nil and error msg already in stack */
      return 2;
    }

  } else { 
    if ( L == mainlp.lstate ) {
      /* sending process is the parent (main) Lua state - block it */
      mainlp.chan = chan;
      luaproc_queue_sender( &mainlp );
      luaproc_unlock_channel( chan );
      pthread_mutex_lock( &mutex_mainls );
      pthread_cond_wait( &cond_mainls_sendrecv, &mutex_mainls );
      pthread_mutex_unlock( &mutex_mainls );
      return mainlp.args;
    } else {
      /* sending process is a standard luaproc - set status, block and yield */
      self = luaproc_getself( L );
      if ( self != NULL ) {
        self->status = LUAPROC_STATUS_BLOCKED_SEND;
        self->chan   = chan;
      }
      /* yield. channel will be unlocked by the scheduler */
      return lua_yield( L, lua_gettop( L ));
    }
  }
}

/* receive a message from a lua process */
static int luaproc_receive( lua_State *L ) {

  int ret, nargs;
  channel *chan;
  luaproc *srclp, *self;
  const char *chname = luaL_checkstring( L, 1 );

  /* get number of arguments passed to function */
  nargs = lua_gettop( L );

  chan = channel_locked_get( chname );
  /* if channel is not found, return an error to Lua */
  if ( chan == NULL ) {
    lua_pushnil( L );
    lua_pushfstring( L, "channel '%s' does not exist", chname );
    return 2;
  }

  /* remove first lua process, if any, from channels' send list */
  srclp = list_remove( &chan->send );

  if ( srclp != NULL ) {  /* found a sender? */
    /* try to move values between lua states' stacks */
    ret = luaproc_copyvalues( srclp->lstate, L );
    if ( ret == TRUE ) { /* was receive successful? */
      lua_pushboolean( srclp->lstate, TRUE );
      srclp->args = 1;
    } else {  /* nil and error_msg already in stack */
      srclp->args = 2;
    }
    if ( srclp->lstate == mainlp.lstate ) {
      /* if sending process is the parent (main) Lua state, unblock it */
      pthread_mutex_lock( &mutex_mainls );
      pthread_cond_signal( &cond_mainls_sendrecv );
      pthread_mutex_unlock( &mutex_mainls );
    } else {
      /* otherwise, schedule process for execution */
      sched_queue_proc( srclp );
    }
    /* unlock channel access */
    luaproc_unlock_channel( chan );
    /* disconsider channel name, async flag and any other args passed 
       to the receive function when returning its results */
    return lua_gettop( L ) - nargs; 

  } else {  /* otherwise test if receive was synchronous or asynchronous */
    if ( lua_toboolean( L, 2 )) { /* asynchronous receive */
      /* unlock channel access */
      luaproc_unlock_channel( chan );
      /* return an error */
      lua_pushnil( L );
      lua_pushfstring( L, "no senders waiting on channel '%s'", chname );
      return 2;
    } else { /* synchronous receive */
      if ( L == mainlp.lstate ) {
        /*  receiving process is the parent (main) Lua state - block it */
        mainlp.chan = chan;
        luaproc_queue_receiver( &mainlp );
        luaproc_unlock_channel( chan );
        pthread_mutex_lock( &mutex_mainls );
        pthread_cond_wait( &cond_mainls_sendrecv, &mutex_mainls );
        pthread_mutex_unlock( &mutex_mainls );
        return mainlp.args;
      } else {
        /* receiving process is a standard luaproc - set status, block and 
           yield */
        self = luaproc_getself( L );
        if ( self != NULL ) {
          self->status = LUAPROC_STATUS_BLOCKED_RECV;
          self->chan   = chan;
        }
        /* yield. channel will be unlocked by the scheduler */
        return lua_yield( L, lua_gettop( L ));
      }
    }
  }
}

/* create a new channel */
static int luaproc_create_channel( lua_State *L ) {

  const char *chname = luaL_checkstring( L, 1 );

  channel *chan = channel_locked_get( chname );
  if (chan != NULL) {  /* does channel exist? */
    /* unlock the channel mutex locked by channel_locked_get */
    luaproc_unlock_channel( chan );
    /* return an error to lua */
    lua_pushnil( L );
    lua_pushfstring( L, "channel '%s' already exists", chname );
    return 2;
  } else {  /* create channel */
    channel_create( chname );
    lua_pushboolean( L, TRUE );
    return 1;
  }
}

/* destroy a channel */
static int luaproc_destroy_channel( lua_State *L ) {

  channel *chan;
  list *blockedlp;
  luaproc *lp;
  const char *chname = luaL_checkstring( L,  1 );

  /* get exclusive access to channels list */
  pthread_mutex_lock( &mutex_channel_list );

  /*
     try to get channel and lock it; if lock fails, release external
     lock ('mutex_channel_list') to try again when signaled -- this avoids
     keeping the external lock busy for too long. during this release,
     the channel may have been destroyed, so it must try to get it again.
  */
  while ((( chan = channel_unlocked_get( chname )) != NULL ) &&
          ( pthread_mutex_trylock( &chan->mutex ) != 0 )) {
    pthread_cond_wait( &chan->can_be_used, &mutex_channel_list );
  }

  if ( chan == NULL ) {  /* found channel? */
    /* release exclusive access to channels list */
    pthread_mutex_unlock( &mutex_channel_list );
    /* return an error to lua */
    lua_pushnil( L );
    lua_pushfstring( L, "channel '%s' does not exist", chname );
    return 2;
  }

  /* remove channel from table */
  lua_getglobal( chanls, LUAPROC_CHANNELS_TABLE );
  lua_pushnil( chanls );
  lua_setfield( chanls, -2, chname );
  lua_pop( chanls, 1 );

  pthread_mutex_unlock( &mutex_channel_list );

  /*
     wake up workers there are waiting to use the channel.
     they will not find the channel, since it was removed,
     and will not get this condition anymore.
   */
  pthread_cond_broadcast( &chan->can_be_used );

  /*
     dequeue lua processes waiting on the channel, return an error message
     to each of them indicating channel was destroyed and schedule them
     for execution (unblock them).
   */
  if ( chan->send.head != NULL ) {
    lua_pushfstring( L, "channel '%s' destroyed while waiting for receiver", 
                     chname );
    blockedlp = &chan->send;
  } else {
    lua_pushfstring( L, "channel '%s' destroyed while waiting for sender", 
                     chname );
    blockedlp = &chan->recv;
  }
  while (( lp = list_remove( blockedlp )) != NULL ) {
    /* return an error to each process */
    lua_pushnil( lp->lstate );
    lua_pushstring( lp->lstate, lua_tostring( L, -1 ));
    lp->args = 2;
    sched_queue_proc( lp ); /* schedule process for execution */
  }

  /* unlock channel mutex and destroy both mutex and condition */
  pthread_mutex_unlock( &chan->mutex );
  pthread_mutex_destroy( &chan->mutex );
  pthread_cond_destroy( &chan->can_be_used );

  lua_pushboolean( L, TRUE );
  return 1;
}

/***********************
 * get'ers and set'ers *
 ***********************/

/* return the channel where a lua process is blocked at */
channel *luaproc_get_channel( luaproc *lp ) {
  return lp->chan;
}

/* return a lua process' status */
int luaproc_get_status( luaproc *lp ) {
  return lp->status;
}

/* set lua a process' status */
void luaproc_set_status( luaproc *lp, int status ) {
  lp->status = status;
}

/* return a lua process' state */
lua_State *luaproc_get_state( luaproc *lp ) {
  return lp->lstate;
}

/* return the number of arguments expected by a lua process */
int luaproc_get_numargs( luaproc *lp ) {
  return lp->args;
}

/* set the number of arguments expected by a lua process */
void luaproc_set_numargs( luaproc *lp, int n ) {
  lp->args = n;
}

/**********************************
 * register structs and functions *
 **********************************/

static void luaproc_reglualib( lua_State *L, const char *name, 
                               lua_CFunction f ) {
  lua_getglobal( L, "package" );
  lua_getfield( L, -1, "preload" );
  lua_pushcfunction( L, f );
  lua_setfield( L, -2, name );
  lua_pop( L, 2 );
}

static void luaproc_openlualibs( lua_State *L ) {
  requiref( L, "_G", luaopen_base, FALSE );
  requiref( L, "package", luaopen_package, TRUE );
  luaproc_reglualib( L, "io", luaopen_io );
  luaproc_reglualib( L, "os", luaopen_os );
  luaproc_reglualib( L, "table", luaopen_table );
  luaproc_reglualib( L, "string", luaopen_string );
  luaproc_reglualib( L, "math", luaopen_math );
  luaproc_reglualib( L, "debug", luaopen_debug );
#if (LUA_VERSION_NUM == 502)
  luaproc_reglualib( L, "bit32", luaopen_bit32 );
#endif
#if (LUA_VERSION_NUM >= 502)
  luaproc_reglualib( L, "coroutine", luaopen_coroutine );
#endif
#if (LUA_VERSION_NUM >= 503)
  luaproc_reglualib( L, "utf8", luaopen_utf8 );
#endif

}

LUALIB_API int luaopen_luaproc( lua_State *L ) {

  /* register luaproc functions */
  luaL_newlib( L, luaproc_funcs );

  /* wrap main state inside a lua process */
  mainlp.lstate = L;
  mainlp.status = LUAPROC_STATUS_IDLE;
  mainlp.args   = 0;
  mainlp.chan   = NULL;
  mainlp.next   = NULL;
  /* initialize recycle list */
  list_init( &recycle_list );
  /* initialize channels table and lua_State used to store it */
  chanls = luaL_newstate();
  lua_newtable( chanls );
  lua_setglobal( chanls, LUAPROC_CHANNELS_TABLE );
  /* create finalizer to join workers when Lua exits */
  lua_newuserdata( L, 0 );
  lua_setfield( L, LUA_REGISTRYINDEX, "LUAPROC_FINALIZER_UDATA" );
  luaL_newmetatable( L, "LUAPROC_FINALIZER_MT" );
  lua_pushliteral( L, "__gc" );
  lua_pushcfunction( L, luaproc_join_workers );
  lua_rawset( L, -3 );
  lua_pop( L, 1 );
  lua_getfield( L, LUA_REGISTRYINDEX, "LUAPROC_FINALIZER_UDATA" );
  lua_getfield( L, LUA_REGISTRYINDEX, "LUAPROC_FINALIZER_MT" );
  lua_setmetatable( L, -2 );
  lua_pop( L, 1 );
  /* initialize scheduler */
  if ( sched_init() == LUAPROC_SCHED_PTHREAD_ERROR ) {
    luaL_error( L, "failed to create worker" );
  }

  return 1;
}

static int luaproc_loadlib( lua_State *L ) {

  /* register luaproc functions */
  luaL_newlib( L, luaproc_funcs );

  return 1;
}
