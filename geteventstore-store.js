/* Copyright (c) 2015 Colm Harte, MIT License */
/* jshint node:true, asi:true, eqnull:true */
"use strict";


var _ = require('lodash');
var gesClient = require('geteventstore-client');


module.exports = function(options) {

  var seneca = this;
  var desc;
  var dbinst = null;

  var name = "geteventstore-store";

  function error(args,err,cb) {
    if (err) {
      seneca.log.error('entity',err,{store:name})
      cb(err)
      return true;
    }
    else
      return false;
  }

  function configure(spec,cb) {

    var dbOpts = seneca.util.deepextend({
      host:"localhost",
      port: 2113
    },spec.options);

    dbinst = new gesClient.createClient(dbOpts);

    seneca.log.debug('init', 'db open', dbOpts);

    cb(null);
  }

  /*
    To find the current version of an object its necessary to iterate through the stream from the earliest item to the latest to get the current state of each entity
  */
  function iterateStreamToFirst(streamName, qq, itemStore, callback)
  {
    dbinst.readPrevious(streamName, function(err, data){
      if (err)
        callback(err);
      else {

        if (data !== null) {

          if (data && isValidRow(qq, data))
            itemStore[data.id] = data;

          if (isDeletedRow(data))
            delete itemStore[data.id];

          iterateStreamToFirst(streamName, qq, itemStore, callback);
        }
        else {
          callback(null);
        }
      }
    });
  }

  /*
    Items in a stream can't be deleted so instead a deleted object is added to the stream to mark an entity as deleted
  */
  function iterateDelete(streamName, list, position, cb) {
    if (position < list.length) {

      var listItem = list[position];
      listItem._deleted = true;
      dbinst.write(streamName, JSON.stringify(listItem.data$(false)), "deleted", function(err, result){
        if (!err) {
          position++;
          iterateDelete(streamName, list, position, cb);
        }
        else {
          cb(err);
        }
      });
    }
    else {
      cb(null);
    }

  }

  var store = {

    save: function(args,cb){

      var ent = args.ent;

      var canon = ent.canon$({object: true});

      var update = !!ent.id;

      if (!update) {
        ent.id = void 0 != ent.id$ ? ent.id$ : -1;

        delete(ent.id$);

        if (ent.id === -1) {
          this.act({role:'util', cmd:'generate_id',
                      name:canon.name, base:canon.base, zone:canon.zone, length: 10 },
                      function(err,id){
                        if (err) return cb(err);

                        ent.id = id;

                        completeSave(id, true);
                      });
        }
        else {
          completeSave(ent.id, false);
        }
      }
      else
        completeSave(ent.id, false);


      function completeSave(id, isNew) {
        var streamName = makeStreamName(ent);

        dbinst.write(streamName, JSON.stringify(ent.data$(false)), isNew ? "created" : "updated", function(err, res) {
          if (!error(args,err,cb)) {
            seneca.log.debug('save/update',ent,desc);

            cb(null,ent);
          }
        })
      }
    },

    load: function(args, cb) {
      //load all entities in the stream in order to get the object requested
      store.list(args, function(err, list){

          if (list.length > 0) {
            cb(err, list[0]);
          }
          else {
            cb(err, null);
          }
      });

    },

    list: function(args,cb){
      var qent = args.qent;
      var q    = args.q;

      var qq = fixquery(qent,q);

      var listItems = {};
      var list = [];

      var streamName = makeStreamName(qent);

      //position read point at the earliest item in the stream
      dbinst.readLast(streamName, function(err, data){
        if (!error(args, err, cb))
        {

          if (data && isValidRow(qq, data) && !isDeletedRow(data)) {
            listItems[data.id] = data;
          }

          iterateStreamToFirst(streamName, qq, listItems, function(err){
            //stream data has been read and list now contains alist of all valid items
            if (err)
              cb(err);
            else
              processList();
          });

        }
      })

      function processList()
      {

        for (var property in listItems) {
            if (listItems.hasOwnProperty(property)) {
              list.push(qent.make$(listItems[property]));
            }
        }
        // sort first
        if (q.sort$) {
          for (var sf in q.sort$) break;
          var sd = qq.sort$[sf] < 0 ? -1 : 1;

          list = list.sort(function(a,b){
            return sd * ( a[sf] < b[sf] ? -1 : a[sf] === b[sf] ? 0 : 1 );
          })
        }

        if (q.skip$) {
          list = list.slice(qq.skip$);
        }

        if (q.limit$) {
          list = list.slice(0,qq.limit$);
        }

        seneca.log.debug('list',q,list.length,list[0],desc);
        cb(null,list);
      }
    },



    remove: function(args,cb){
      var qent = args.qent
      var q    = args.q

      var all  = q.all$ // default false
      var load  = _.isUndefined(q.load$) ? true : q.load$ // default true

      var qq = fixquery(qent, q)

      var streamName = makeStreamName(qent);

      if (all) {
        //for each item in the list add a new deleted entry to the stream
        store.list(args, function(err, list){
          iterateDelete(streamName, list, 0, function(err){
            seneca.log.debug('remove/all',q,desc);
            cb(err);
          });
        });


      }
      else {

        store.load(args, function(err, data){

            if (!error(args, err, cb))
            {

              if (data && data.id) {

                data._deleted = true;

                //can't delete an item in a stream so add a new entry with same id and mark it as deleted
                dbinst.write(streamName, JSON.stringify(data.data$(false)), "deleted", function(err, result){
                  if (!error(args, err, cb))
                  {
                    seneca.log.debug('remove/one', q, data, desc);

                    var ent = load ? data : null;
                    cb(err,ent);
                  }
                });
              }
            }

          });
      }

    },


    close: function(args,cb){
      this.log.debug('close',desc)
      cb()
    },


    native: function(args,cb){
      cb(null,dbinst)
    }
  }



  var meta = this.store.init(this,options,store)

  desc = meta.desc

  options.idlen = options.idlen || 10

  this.add({role:name,cmd:'dump'},function(args,cb){
    cb(null,entmap)
  })

  this.add({role:name,cmd:'export'},function(args,done){
    var entjson = JSON.stringify(entmap)
    fs.writeFile(args.file,entjson,function(err){
      done(err,{ok:!!err})
    })
  })


  this.add({role:name,cmd:'import'},function(args,done){
    try {
      fs.readFile(args.file,function(err,entjson){
        if( entjson ) {
          try {
            entmap = JSON.parse(entjson)
            done(err,{ok:!!err})
          }
          catch(e){
            done(e)
          }
        }
      })
    }
    catch(e){
      done(e)
    }
  })

  var meta = seneca.store.init(seneca,options,store)
  desc = meta.desc

  seneca.add({init:name,tag:meta.tag},function(args,done){
    configure(options,function(err){
      if (err) return seneca.die('store',err,{store:name,desc:desc});
      return done();
    })
  })


  return {name:name,tag:meta.tag}
}


function makeStreamName(ent, id) {
  var canon = ent.canon$({object: true});

  return (canon.base ? canon.base + '_' : '') + canon.name;

}

function isValidRow(q, data)
{

  for(var p in q) {

    if( !~p.indexOf('$') && q[p] != data[p]) {
      return false;
    }
  }

  return true;

}

function isDeletedRow(data)
{
  return data._deleted;
}

function fixquery(qent, q) {
  return null==q ? {} : _.isString(q) ? {id: q} : _.isString(q.id) ? q : q;
}
