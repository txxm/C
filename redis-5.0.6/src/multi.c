/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"

/* ================================ MULTI/EXEC ============================== */

/* 事务初始化 */
void initClientMultiState(client *c) {
    c->mstate.commands = NULL;
    c->mstate.count = 0;
    c->mstate.cmd_flags = 0;
}

/* 释放与事务有关的资源 */
void freeClientMultiState(client *c) {
    int j;

    /* 1)释放事务命令队列中每一个命令的资源 */
    for (j = 0; j < c->mstate.count; j++) {     /* 遍历按每一个事务命令 */
        int i;

        /* 1.1)获取命令的地址 */
        multiCmd *mc = c->mstate.commands+j;

        /* 1.2)减少每个命令的引用计数 */
        for (i = 0; i < mc->argc; i++)          /* 遍历命令中的每一个字符串 */
            decrRefCount(mc->argv[i]);          /* 递减该字符串的引用计数 */

        /* 1.3)释放与命令有关的资源 */
        zfree(mc->argv);
    }

    /* 2)释放与事务命令组相关的资源 */
    zfree(c->mstate.commands);
}

/* 把新的命令加入到事务命令数组中 */
void queueMultiCommand(client *c) {
    multiCmd *mc;
    int j;

    /* 1)增加事务命令队列的空间，容纳新的命令 */
    c->mstate.commands = zrealloc(c->mstate.commands,
            sizeof(multiCmd)*(c->mstate.count+1));

    /* 2)获取新命令的存放地址 */
    mc = c->mstate.commands+c->mstate.count;

    /* 3)设置参数列表、参数数量、命令函数 */
    mc->cmd = c->cmd;
    mc->argc = c->argc;                                 /* 该命令的字符串个数 */
    mc->argv = zmalloc(sizeof(robj*)*c->argc);          /* 为每个字符串分配空间 */
    memcpy(mc->argv,c->argv,sizeof(robj*)*c->argc);     /* 将命令复制到multiCmd结构中 */

    /* 4)增加参数列表中每个参数的引用计数 */
    for (j = 0; j < c->argc; j++)                       /* 遍历命令参数列表中每个参数 */
        incrRefCount(mc->argv[j]);                      /* 增加每个参数的引用计数 */

    /* 5)增加事务命令队列中命令的数量 */
    c->mstate.count++;
    c->mstate.cmd_flags |= c->cmd->flags;
}

/* 取消事务 */
void discardTransaction(client *c) {
    /* 1)释放与事务相关的资源 */
    freeClientMultiState(c);

    /* 2)重新初始化事务 */
    initClientMultiState(c);

    /* 3)取消与事务相关的标志位：MULTI(开启事务)、CAS(防止key被修改)、DIRTY_EXEC(静态错误，即命令出错) */
    c->flags &= ~(CLIENT_MULTI|CLIENT_DIRTY_CAS|CLIENT_DIRTY_EXEC);

    /* 4)取消客户端监视队列中对所有键的监控 */
    unwatchAllKeys(c);
}

/* 当客户端加入的命令有错误的时候调用该函数，将阻止EXEC命令执行 */
void flagTransaction(client *c) {
    if (c->flags & CLIENT_MULTI)            /* 判断是否开启事务 */
        c->flags |= CLIENT_DIRTY_EXEC;      /* 静态错误标志（如错误的命令），将阻止事务执行 */
}

/* 开启事务：执行MULTI命令 */
void multiCommand(client *c) {
    /* 1)判断是否开启事务 */
    if (c->flags & CLIENT_MULTI) {
        addReplyError(c,"MULTI calls can not be nested");
        return;
    }

    /* 2)置位事务开启标志 */
    c->flags |= CLIENT_MULTI;
    addReply(c,shared.ok);      /* 回复OK（"+OK\r\n"） */
}

/* 取消命令执行 */
void discardCommand(client *c) {
    /* 1)判断是否开启事务 */
    if (!(c->flags & CLIENT_MULTI)) {
        addReplyError(c,"DISCARD without MULTI");
        return;
    }

    /* 2)取消事务 */
    discardTransaction(c);
    addReply(c,shared.ok);
}

/* Send a MULTI command to all the slaves and AOF file. Check the execCommand
 * implementation for more information. */
/* 发送事务命令到所有的节点和AOF文件 */
void execCommandPropagateMulti(client *c) {
    robj *multistring = createStringObject("MULTI",5);

    propagate(server.multiCommand,c->db->id,&multistring,1,
              PROPAGATE_AOF|PROPAGATE_REPL);
    decrRefCount(multistring);
}

/* EXEC命令执行事务流程 */
void execCommand(client *c) {
    int j;
    robj **orig_argv;
    int orig_argc;
    struct redisCommand *orig_cmd;
    int must_propagate = 0;             /* 决定是否需要传播到AOF文件或其他节点? */
    int was_master = server.masterhost == NULL;

    /* 1)判断事务是否被开启 */
    if (!(c->flags & CLIENT_MULTI)) {
        addReplyError(c,"EXEC without MULTI");
        return;
    }

    /* 2)检查是否需要中断事务执行，如:CAS错误，静态错误（DIRTY_EXEC）。
     * CAS错误：某些监控的键被修改（CAS错误），实际不是一个错误，而是一种特殊的表现，返回事务空对象.
     * 静态错误：加入的命令有错误（静态错误），返回ABORT错误. */
    if (c->flags & (CLIENT_DIRTY_CAS|CLIENT_DIRTY_EXEC)) {      /* 判断是否设置CAS错误或静态错误标志，若设置，中断事务的执行 */
        addReply(c, c->flags & CLIENT_DIRTY_EXEC ? shared.execaborterr :
                                                  shared.nullmultibulk);
        discardTransaction(c);
        goto handle_monitor;
    }

    /* 3)如果事务中有写命令，并且这是一个只读从设备，则我们要发送错误。
     * 当实例是主实例或可写副本时启动事务，然后更改配置（例如，实例变成副本）时，就会发生这种情况。*/
    if (!server.loading && server.masterhost && server.repl_slave_ro &&
        !(c->flags & CLIENT_MASTER) && c->mstate.cmd_flags & CMD_WRITE)
    {
        addReplyError(c,
            "Transaction contains write commands but instance "
            "is now a read-only slave. EXEC aborted.");
        discardTransaction(c);
        goto handle_monitor;
    }

    /* 4)取消客户端监视队列中对所有键的监控 */
    unwatchAllKeys(c);          /* 由于Redis是单线程，可以取消监控所有的键，避免浪费CPU资源 */

    /* 5)备份EXEC要执行的命令 */
    orig_argv = c->argv;        /* 将该客户端命令参数复制到临时变量 */
    orig_argc = c->argc;        /* 该客户端命令参数个数复制到临时变量 */
    orig_cmd = c->cmd;

    /* 6)回复事务中命令的数量 */
    addReplyMultiBulkLen(c,c->mstate.count);

    /* 7)执行事务队列中的每个命令 */
    for (j = 0; j < c->mstate.count; j++) {     /* 遍历事务命令组中的每个命令 */
        c->argc = c->mstate.commands[j].argc;   /* 当前要执行的命令参数个数 */
        c->argv = c->mstate.commands[j].argv;   /* 当前要执行的命令参数 */
        c->cmd = c->mstate.commands[j].cmd;

        /* Propagate a MULTI request once we encounter the first command which
         * is not readonly nor an administrative one.
         * This way we'll deliver the MULTI/..../EXEC block as a whole and
         * both the AOF and the replication link will have the same consistency
         * and atomicity guarantees. */
        if (!must_propagate && !(c->cmd->flags & (CMD_READONLY|CMD_ADMIN))) {
            execCommandPropagateMulti(c);
            must_propagate = 1;
        }

        call(c,server.loading ? CMD_CALL_NONE : CMD_CALL_FULL);

        /* Commands may alter argc/argv, restore mstate. */
        c->mstate.commands[j].argc = c->argc;
        c->mstate.commands[j].argv = c->argv;
        c->mstate.commands[j].cmd = c->cmd;
    }

    /* 8)还原EXEC要执行的命令 */
    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;

    /* 9)取消事务 */
    discardTransaction(c);

    /* 10)如果MULTI事务被传播，"EXEC"命令也要被传播 */
    if (must_propagate) {                                           /* 判断事务是否被传播 */
        int is_master = server.masterhost == NULL;
        server.dirty++;
        /* If inside the MULTI/EXEC block this instance was suddenly
         * switched from master to slave (using the SLAVEOF command), the
         * initial MULTI was propagated into the replication backlog, but the
         * rest was not. We need to make sure to at least terminate the
         * backlog with the final EXEC. */
        if (server.repl_backlog && was_master && !is_master) {
            char *execcmd = "*1\r\n$4\r\nEXEC\r\n";
            feedReplicationBacklog(execcmd,strlen(execcmd));
        }
    }

    /* 11)监控器处理 */
handle_monitor:
    if (listLength(server.monitors) && !server.loading)                         /* 服务端设置监控器，并且服务器不处于载入文件状态 */
        replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);   /* 将参数列表信息发送到服务器的监控器 */
}

/* ===================== WATCH (CAS alike for MULTI/EXEC) ===================
 *
 * The implementation uses a per-DB hash table mapping keys to list of clients
 * WATCHing those keys, so that given a key that is going to be modified
 * we can mark all the associated clients as dirty.
 *
 * Also every client contains a list of WATCHed keys so that's possible to
 * un-watch such keys when the client is freed or when UNWATCH is called. */

/* In the client->watched_keys list we need to use watchedKey structures
 * as in order to identify a key in Redis we need both the key name and the
 * DB */
/* 关联被监视的键和该键所在的数据库 */
typedef struct watchedKey {
    robj *key;      /* 被监视的键 */
    redisDb *db;    /* 键所在的数据库 */
} watchedKey;

/* 监控指定的键，该键需要添加到两部分：
 * 1)数据库监视字典
 * 2)客户端监视队列 */
void watchForKey(client *c, robj *key) {
    list *clients = NULL;
    listIter li;
    listNode *ln;
    watchedKey *wk;

    /* 1)判断该键是否在已经被监视 */
    listRewind(c->watched_keys,&li);                                /* 获取客户端监视队列 */
    while((ln = listNext(&li))) {
        wk = listNodeValue(ln);                                     /* 取出链表的节点，即客户端监视队列中的key */
        if (wk->db == c->db && equalStringObjects(key,wk->key))     /* 比较watchedKey结构中的db和key字段是否相等，如果相等，说明该键被监视 */
            return;                                                 /* 键被监视，直接返回 */
    }

    /* 2)将该键添加到数据库监视队列中 */
    clients = dictFetchValue(c->db->watched_keys,key);              /* 获取数据库监视字典中key对应的值，如果为空，说明数据库监视字典中没有添加该键 */
    if (!clients) {
        clients = listCreate();                                     /* 创建并初始化一个存放客户端的队列 */
        dictAdd(c->db->watched_keys,key,clients);                   /* 将该键以及客户端队列添加到数据库监视字典中 */
        incrRefCount(key);                                          /* 增加该键的引用计数 */
    }
    listAddNodeTail(clients,c);                                     /* 将客户端添加到数据库监视字典key所对应的队列中 */
    
    /* 3)将该键添加到客户端监视队列中 */
    wk = zmalloc(sizeof(*wk));                                      /* 为该键申请空间 */
    wk->key = key;
    wk->db = c->db;
    incrRefCount(key);
    listAddNodeTail(c->watched_keys,wk);                            /* 将watchedKey结构添加到客户端监视队列的尾部 */
}

/* 取消所有被客户端监视的键. */
void unwatchAllKeys(client *c) {
    listIter li;
    listNode *ln;

    /* 1)判断该键是否被监视 */
    if (listLength(c->watched_keys) == 0) return;                   /* 在客户端监视队列中，如果该键没有被监视，直接返回 */

    /* 2)取消对所有被监视的键的监控 */
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {                                   /* 遍历客户端监视队列中的键 */
        list *clients;
        watchedKey *wk;

        /* 2.1)删除数据库监视字典中的客户端 */
        wk = listNodeValue(ln);
        clients = dictFetchValue(wk->db->watched_keys, wk->key);    /* 从数据库监视字典中取出该键对应的值(存放客户端的队列) */
        serverAssertWithInfo(c,NULL,clients != NULL);
        listDelNode(clients,listSearchKey(clients,c));              /* 从数据库监视字典中该key所对应的队列中删除该客户端 */

        /* 2.2)是否删除数据库监视队列中的key */
        if (listLength(clients) == 0)                               /* 判断数据库监视字典中该键的值是否为空，如果为空则说明没有客户端监视该键 */
            dictDelete(wk->db->watched_keys, wk->key);              /* 从数据库监视字典中删除该键 */

        /* 2.3)删除客户端监视队列中的该键 */
        listDelNode(c->watched_keys,ln);                            /* 从该客户端的监控队列中删除该节点 */
        decrRefCount(wk->key);                                      /* 减少该键的引用计数 */
        zfree(wk);
    }
}

/* 扫描服务器的监视字典，检查键key是否被某些客户端监视，如果有，将对应客户端标记为事务破坏状态 */
void touchWatchedKey(redisDb *db, robj *key) {
    list *clients;
    listIter li;
    listNode *ln;

    /* 1)检查该键是否被监视 */
    if (dictSize(db->watched_keys) == 0) return;        /* 如果数据库监视字典中没有键被监视，立刻返回 */
    clients = dictFetchValue(db->watched_keys, key);    /* 从数据库监视字典中取出key对应的值（存放客户端的队列） */
    if (!clients) return;

    /* 2)将数据库监视字典中key所对应的所有客户端设置为CAS标志 */
    listRewind(clients,&li);
    while((ln = listNext(&li))) {                       /* 遍历监视key的所有客户端 */
        client *c = listNodeValue(ln);                  /* 取出节点的值，即客户端 */

        c->flags |= CLIENT_DIRTY_CAS;                   /* 设置CAS标志，表示事务执行时失败 */
    }
}

/* On FLUSHDB or FLUSHALL all the watched keys that are present before the
 * flush but will be deleted as effect of the flushing operation should
 * be touched. "dbid" is the DB that's getting the flush. -1 if it is
 * a FLUSHALL operation (all the DBs flushed). */
void touchWatchedKeysOnFlush(int dbid) {
    listIter li1, li2;
    listNode *ln;

    /* For every client, check all the waited keys */
    /* 1)遍历服务器的客户端队列 */
    listRewind(server.clients,&li1);
    while((ln = listNext(&li1))) {
        client *c = listNodeValue(ln);

        /* 1.1)遍历每个客户端监视队列 */
        listRewind(c->watched_keys,&li2);
        while((ln = listNext(&li2))) {
            watchedKey *wk = listNodeValue(ln);

            /* For every watched key matching the specified DB, if the
             * key exists, mark the client as dirty, as the key will be
             * removed. */
            /* 1.1.1)将与该key对应的数据库的客户端标记为CAS状态 */
            if (dbid == -1 || wk->db->id == dbid) {                 /* 判断被监视的数据库的ID和dbid是否相同 */
                if (dictFind(wk->db->dict, wk->key->ptr) != NULL)   /* 判断被监视的键是否存在 */
                    c->flags |= CLIENT_DIRTY_CAS;                   /* 设置CAS标志 */
            }
        }
    }
}

/* 监控功能的实现 */
void watchCommand(client *c) {
    int j;

    /* 1)判断是否开启事务 */
    if (c->flags & CLIENT_MULTI) {      /* 判断是否开启事务，如果开启，返回错误 */
        addReplyError(c,"WATCH inside MULTI is not allowed");
        return;
    }

    /* 2)添加WATCH命令参数 */
    for (j = 1; j < c->argc; j++)       /* 遍历WATCH命令的参数 */
        watchForKey(c,c->argv[j]);      /* 将WATCH命令的每个参数添加到数据库监视字典和客户端监视队列中 */
    addReply(c,shared.ok);
}

/* 取消监控 */
void unwatchCommand(client *c) {
    /* 1)取消客户端监视队列中所有被监视的键 */
    unwatchAllKeys(c);
    
    /* 2) 清除CAS标志*/
    c->flags &= (~CLIENT_DIRTY_CAS);
    addReply(c,shared.ok);
}
