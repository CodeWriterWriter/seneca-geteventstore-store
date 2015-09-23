"use strict";


var seneca = require('seneca')

var shared = require('seneca-store-test')

var si = seneca({log:'silent'})
si.use('../geteventstore-store.js')

var Lab = require('lab');
var lab = exports.lab = Lab.script()

var describe = lab.describe

describe('geteventstore', function(){
  shared.basictest({
    seneca: si,
    script: lab
  });

  shared.sorttest({
   seneca: si,
    script: lab
  });

  shared.limitstest({
    seneca: si,
    script: lab
  });

})
