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

/***********************  INITIALIZATIONS ********************************/

const port = process.env.port || 3002;
console.log('Running at Port 3002');

server.listen(port, () => {
    console.log('Server is up!');
});

io.on('connection', function (socket) {
    socket.on("hello", (msg) => {
        log("hello there ..." + msg);
        return true;
    });
});

var sockout = csocket.connect('http://localhost:3001');

sockout.emit("hello", "from 3002" );
