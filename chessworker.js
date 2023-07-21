/**************************************************************************/
//   Copyright (c) Raymond Michael O. Ordona (2022)
//   Chess Server
/************************ IMPORT LIBRARIES ********************************/
const express     = require('express'); 
const bodyParser  = require('body-parser');
const fs          = require('fs');
const formidable  = require('formidable');
const url     = require('url');
const peg     = require("pegjs");
const http    = require("http");
const socket  = require("socket.io");
const csocket = require('socket.io-client');
const bcrypt  = require("bcrypt");
const { Pool }  = require("pg");
const jwt     = require('jsonwebtoken');
const cookieParser = require('cookie-parser');
// const { Command } = require('commander');
// const program = new Command();
    
const app    = express(); 
const path   = require('path'); 
const router = express.Router(); 
  
const server = http.createServer(app);
const io     = socket(server);

app.use(cookieParser());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }))
app.use('/images', express.static('images'));
app.use('/audio', express.static('audio'));
app.use('/jscripts', express.static('jscripts'));
app.use('/socket.io', express.static('socket.io'));
app.use('/', router);

var log = function(msg) { console.log(msg) };

const options_ = { "-p" : { desc: "Listening Port", val: null }  }

process.argv.forEach(function (val, index, array) {
  if (val.match("=")) {
     var arg = val.split('=');
     var option = options_[arg[0]];
     if (option != null) {
         options_[arg[0]].val = arg[1];      
     } else {
        log("error option: " + val);
        process.exit(1)
     }
  }
});

var portarg = parseInt(options_["-p"].val);

/***********************  INITIALIZATIONS ********************************/

var port = 3001;
// const port = process.env.port || portarg;

function startserver(port) {
    server.listen(port, () => {
         log('Running at Port ' + port);
         log('Server is up!');
         return { code: 0 };
    }).on('error', (e) => {
       if (e.code === 'EADDRINUSE') {
           log("xxx");
           server.close();
           startserver(port + 1);
           return { code: 1 }
       }
    });
}

startserver(port);

/*
io.on('connection', function (socket) {
      socket.on("hello", (msg) => {
        log("hello there ..." + msg); 
        return true;
      });
});
*/

// var sockout = csocket.connect('http://localhost:3002');

// sockout.emit("hello", "from 3001" );
