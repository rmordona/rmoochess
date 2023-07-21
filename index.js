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
const bcrypt  = require("bcrypt");
const { Pool }  = require("pg");
const jwt     = require('jsonwebtoken');
const cookieParser = require('cookie-parser');
    
const app    = express(); 
const path   = require('path'); 
const router = express.Router(); 
  
const server = http.createServer(app);
const io     = socket(server);

/***********************  INITIALIZATIONS ********************************/
    
    /*
    // create application/json parser
    var jsonParser = bodyParser.json()
     
    // create application/x-www-form-urlencoded parser
    var urlencodedParser = bodyParser.urlencoded({ extended: false })
    */
    
    // log
    function log(msg) { console.log(msg); }
    
    // set timezone immediately.
    // TZ="UTC";
    // process.env.TZ = 'UTC'; 
    const UTCdate   = function(dt) { return (new Date(new Date(dt).toUTCString().replace(/ GMT/,'')) / 1); };
    const UTCtoday  = function() { return (new Date(new Date().toUTCString().replace(/ GMT/,'')) / 1); };
    const LOCALdate = function(dt) { return daylocal(daylocal(dt) + ' GMT'); };
    
    var today = Date.now();
    var server_restart_date = Date.now(); // if server restarts, recover cached data.
    var server_restart = true;

    // Schedule timer
    SCHED_TIMER = 10000; /* 10 seconds */
    
    // Prepare Sessions
    var   GUEST      = 201;
    var   REGISTERED = 202;
    const GUESTSESSION_ = {}; // index by session id
    const SESSION_ = {}; // index by session id
    const CONFIG_  = {}; // index by session id
    const Session  = {
         username: '',
         firstname: '',
         lastname: ''
    }
    
    // Player sides
    const WHITE        =  97;
    const BLACK        =  98;
    
    // Ready for Game
    const GameQueue = [];
    const PlayerInQueue = {};
    
    // Prepare Games
    const Matches = {};
    const Tourneys = {};
    const Simul = {};
    const Simul_Players = {};
    const Simul_Hosts = {};
    const ForfeitQueue = {};
    
    const possible_ranges =  [
               { mi: 800, ma: 900 },   { mi:  800, ma: 1000 },
               { mi: 900, ma: 1000 },  { mi:  900, ma: 1100 },
               { mi: 1000, ma: 1100 }, { mi: 1000, ma: 1200 },
               { mi: 1100, ma: 1200 }, { mi: 1100, ma: 1300 },
               { mi: 1200, ma: 1300 }, { mi: 1200, ma: 1400 },
               { mi: 1300, ma: 1400 }, { mi: 1300, ma: 1500 },
               { mi: 1400, ma: 1500 }, { mi: 1400, ma: 1600 },
               { mi: 1500, ma: 1600 }, { mi: 1500, ma: 1700 },
               { mi: 1600, ma: 1700 }, { mi: 1600, ma: 1800 },
               { mi: 1700, ma: 1800 }, { mi: 1700, ma: 1900 },
               { mi: 1800, ma: 1900 }, { mi: 1800, ma: 1200 },
               { mi: 1900, ma: 2000 }, { mi: 1900, ma: 2100 },
               { mi: 2000, ma: 2100 }, { mi: 2000, ma: 2200 },
               { mi: 2100, ma: 2200 }, { mi: 2100, ma: 2300 },
               { mi: 2200, ma: 2300 }, { mi: 2200, ma: 2400 },
               { mi: 2300, ma: 2400 }, { mi: 2300, ma: 2500 },
               { mi: 2400, ma: 2500 }, { mi: 2400, ma: 2600 },
               { mi: 2500, ma: 2600 }, { mi: 2500, ma: 2700 },
               { mi: 2600, ma: 2700 }, { mi: 2600, ma: 2800 },
               { mi: 2700, ma: 2800 }, { mi: 2700, ma: 2900 },
               { mi: 2800, ma: 2900 }, { mi: 2800, ma: 3000 },
               { mi: 2900, ma: 3000 }, { mi: 2900, ma: 3100 },
               { mi: 3000, ma: 3100 }, { mi: 3000, ma: 3200 },
               { mi: 3100, ma: 3200 }, { mi: 3100, ma: 3300 },
               { mi: 3200, ma: 3300 }, { mi: 3300, ma: 3400 },
               { mi: 3400, ma: 3500 }, { mi: 3500, ma: 3600 },
               { mi: 3600, ma: 3700 }, { mi: 3700, ma: 3800 },
               { mi: 3800, ma: 4000 } ];
    const possible_rating = [ 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900,
                                      2000, 2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900, 3000, 3100, 3200, 3300,
                                      3400, 3500 ];
    const possible_movetime = [10,20,30,40,50,75,100,200,500,1000,1200,1400,1500,1700,2000,2400,2600,2800,3000,
                                    3200,3400,3600,3800,4000,4500,5000,5500,6000,6500,7000,7500,8000,8500,9000,9500, 9900];
    const possible_depth = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20];
    
    const possible_timer = ['&half;', 1,'1&half;',2,'2&half;', 3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,25,30,35,40,45,50,
                                     55,60,65,70,75,80,90,100,120,140,150,160,170,180,200,230,260,300 ];
    const possible_inc = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,25,30,35,40,45,50,55,60,70,80,90,100,120 ];

    const possible_simul = [1,2,3,4,5,6,7,8 ];

    const recvariants = ['Standard', 'Chess960', 'Crazy House', 'King Of The Hill', 'Three Checks', 'Atomic', 'Horde', 
			'The Mole', 'The Traitor', 'AntiChess'];

    const compvariants = [
                    { code: '', item: 'Standard', initial: 'Std' },
                    { code: '', item: 'Chess960', initial: 'c960' },
                    { code: '', item: 'The Mole', initial: 'mole' },
                    { code: '', item: 'The Traitor', initial: 'traitor' }
                ];

    const timer_types = ['Bullet', 'Blitz', 'Rapid', 'Classical'];

    const GAMEOVER = { CHECKMATE: 1100, STALEMATE: 1200, ABANDONED: 1300, TIMEOUT: 1400, REPETITION: 1500, DRAWACCEPTED: 1600, RESIGNED: 1700,
                       INSUFFICIENT: 1800, KINGOFTHEHILL: 1900, THREECHECKS: 2000, ATOMIC: 2001, HORDE: 2002, ABORTED: 2003, FORFEIT: 2004 };

    const tourneytypes = [
                    { code: '', item: 'Ladder Style', initial: 'LS' },
                    { code: '', item: 'Simultaneous Style', initial: 'SI' },
                    { code: '', item: 'Round robin', initial: 'RR' },
                    { code: '', item: 'Swiss system', initial: 'SS' },
                    { code: '', item: 'Single elimination', initial: 'SE' },
                    { code: '', item: 'Double elimination', initial: 'DE' },
                    { code: '', item: 'Scheveningen system', initial: 'SC' }
                   ];

    // Prepare PostGres client
    const client = new Pool({
           database: 'postgres',
           user: 'postgres',
           password: 'welcome1',
           host: 'localhost',
           port: 5432,
           max: 25
        });
    
    // Handle PEG translation
    try {
        var grammar = fs.readFileSync('jscripts/pgn.pegjs', 'utf8');
    } catch(e) {
        console.log('Error:', e.stack);
    }
    
    function simplify(pgngame) {
          const Games = [];
            class Game {
               constructor (game) {
                 this.detail_ = { ev   : game[3], si   : game[9], dt   : game[15], round  : game[21], wp  : game[27],
                           bp  : game[33], rt : game[39], we : game[45], be : game[51], eco      : game[57],
                           moves: game[game.length-1].replace(/\r\n/g," ").replace(/\t/g,"").replace(/\n/g," ") }
               }
               get detail()  { return this.detail_; }
            }
    
            for (let i=0; i < pgngame.length; i++) {
                 const game = new Game(pgngame[i])
                 if (game.detail.we != "" && game.detail.be != "" &&  game.detail.eco != "") {
                    const we = parseInt(game.detail.we), be = parseInt(game.detail.be);
                    if (we >=2400 && be >=2400) Games.push(game.detail)
                 }
            }
        return Games;
    }
    
    // Static Images
    
    /*************************************** BEGIN OF APP ROUTING ******************************************/
    
    app.use(cookieParser());
    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({ extended: true }))
    app.use('/images', express.static('images'));
    app.use('/audio', express.static('audio'));
    app.use('/jscripts', express.static('jscripts'));
    app.use('/socket.io', express.static('socket.io'));
    
    router.get('/wasm/*', function(req, res, next) { 
      var pgn_path = __dirname + url.parse(req.url).pathname;
      if (pgn_path.match(/stockfish.js/)) {
         console.log(pgn_path);
         var stockfishjs = fs.readFileSync(pgn_path, 'utf8');
         res.header("Cross-Origin-Embedder-Policy", "require-corp");
         res.header("Cross-Origin-Opener-Policy", "same-origin");
         res.setHeader("Content-Type", "application/javascript"); 
         res.writeHead(200);
         res.end(stockfishjs);
      } else {
         var wasm = new Uint8Array(fs.readFileSync(pgn_path))
          res.header("Cross-Origin-Embedder-Policy", "require-corp");
          res.header("Cross-Origin-Opener-Policy", "same-origin");
         res.setHeader("Content-Type", "application/wasm"); 
         res.writeHead(200);
         res.end(wasm);
      }
    });
    
    
    // Send PGN Json
    router.get('/pgn/*', function(req, res) { 
       var pgn_path = url.parse(req.url).pathname, pgn_json = null;
        pgn_path = pgn_path.substring(1);
         try {
            var pgn = fs.readFileSync(pgn_path, 'utf8');
            pgn = pgn.toString()
           // Get translated PGN
           const parser = peg.generate(grammar);
           pgn_json = parser.parse(pgn);
         } catch(e) {
           console.log('Error:', e.stack);
         }
        var games = simplify(pgn_json);
        res.end(JSON.stringify(games));
    }); 
    
    
    router.get('/favicon.ico', function(req, res) { 
        res.sendFile(path.join(__dirname + '/favicon.ico')); 
    }); 
    
    router.get('/about', function(req, res) { 
        res.sendFile(path.join(__dirname + '/about.html')); 
    }); 
    
    router.get('/sitemap', function(req, res) { 
        res.sendFile(path.join(__dirname + '/sitemap.html')); 
    }); 
    
    router.get('/watch', function(req, res) { 
        res.sendFile(path.join(__dirname + '/watcher.html')); 
    }); 
    
    router.get('/broadcast', function(req, res) { 
        res.sendFile(path.join(__dirname + '/broadcaster.html')); 
    }); 
    
    
    router.post('/upload/photo', function(req, res) {
          const form = new formidable.IncomingForm({ multiples: false, maxFileSize: 264 * 1024 });
          const photo_dir = path.join(__dirname, 'images', 'photo');
    log("got in here ...");
    log(photo_dir);
          form.uploadDir = photo_dir;
          form.parse(req, (err, profile, files) => {
    log("fields ...");
       log(profile);
       log(files);
              if (profile.file == 'undefined') { // no photo to upload
                    return res.status(200).json('No photo to upload!' );
              }
              if (err) {
                log('Error parsing ...');
                return res.status(481).json({ code: 0, msg: 'Failed to parse file ...' }); 
              } else {
                    var found_issue = true;
                    var ext_expr  = /(\.svg|\.png|\.gif|\.jpg|\.jpeg)$/i;
                    var mimetype_expr = /^image\/(svg|svg\+xml|png|gif||jpg|jpeg)$/i;
                    if (files.file.originalFilename.match(ext_expr) && files.file.mimetype.match(mimetype_expr)) {
                       if (parseInt(files.file.size) / 1024 < 264) { /* less than 264kb */
                          found_issue = false;
                       }
                    }
    log("found true?");
    log(found_issue);
                    if (found_issue) return res.status(480).json({ code: 480, msg: 'Invalid file image or file size ...' });
    
    log("going to insert photo ...");
                 const qres = savePhoto(profile, files, photo_dir);
                 qres.then((pres) => {
        log("going to insert photo done ...");
                    if (!qres.code) {
                       return res.status(200).json(pres);
                     } else {
                       return  res.status(480).json(pres);
                     }
                    })
    log("async done ...");
              }
          });
        // res.send('Photo uploaded ....');
    });

    router.post('/machinescore', async function(req, res) {
          var body = req.body;
     log("guest score ...");
          var game = body.game;
          var tourney = body.tourney;
          var sess = body.session;
          session = await authSession(sess.sessionid, sess.username);
          if (session != null) {
              var nrating = calculateRating(game, tourney, wgames_so_far = 0, bgames_so_far = 0, machine = true);
              res.send({ sessionid: sess.sessionid, username: sess.username, game: game, nrating: nrating } );
           }
    });

    
    router.post('/guest', async function(req, res) {
         var chess_       = req.cookies.chess_;
         var hosting_     = req.cookies.hosting_;
         var playinggame_ = req.cookies.playinggame_;
         var simul_       = req.cookies.simul_;
         var socket       = req.body.socket;
log("do we have a session cookie?");
log(chess_);
log("socket");
log(socket);
log("no???");
log(typeof(chess_));
         var game = null, hostinggame = null;
         if (hosting_ != null) {
             hostinggame = checkGame(hosting_);
         } 
         if (playinggame_ != null) {
             game = checkGame(playinggame_);
         }
         if (chess_ == null) {
            const new_session = createSession(GUEST);
            // SESSION_[new_session.username] = new_session;
            new_session.socketid = socket.socketid;
            new_session.utype = GUEST;
            setSession(new_session.username, new_session);
            res.cookie('chess_', JSON.stringify({ sessionid: new_session.sessionid, username: new_session.username, utype: GUEST }) );
            res.send({ token: new_session.sessionid, username: new_session.username, utype: GUEST } );
log("no ... created a new one ");
            return;
         } else {
            if (typeof(chess_) == "string") { 
log("parse maybe ...");
               try {
                  chess_ = JSON.parse(chess_); 
               } catch(err) {
log(err);
               }
            }
            if (chess_.sessionid == null) {
               if (chess_.username != null) {
log("yes ... but no session id ");
                   const session = getSession(chess_.username); // SESSION_[chess_.username];
                   if (session != null)  {
                        const utype = (chess_.utype != null) ? chess_.utype : GUEST;
log("create a new session ...");
                        session.socketid = socket.socketid;
                        session.utype = utype;
                        setSession(chess_.username, session);
                        res.cookie('chess_', JSON.stringify({ sessionid: session.sessionid, username: session.username, utype: utype }) );
                        res.send({ token: session.sessionid, username: session.username, utype: chess_.utype } );
    log(SESSION_);
                        return;
                   }
               }
log("it also does not have a user id ...");
               const new_session = createSession(GUEST);
               const utype = (chess_.utype != null) ? chess_.utype : GUEST;
log("create a new session ...");
               new_session.socketid = socket.socketid;
               new_session.utype = utype;
               setSession(new_session.username, new_session);
               
               res.cookie('chess_', JSON.stringify({ sessionid: new_session.sessionid, username: new_session.username, utype: utype }) );
               res.send({ token: new_session.sessionid, username: new_session.username, utype: utype } );
    log(SESSION_);
               return;
            } else {
               var session = null;
               if (chess_.utype == REGISTERED) {
log("yes ... we also have a session id ... and this is a registered user ...");
                  session = await authSession(chess_.sessionid, chess_.username);
                  if (session != null) {
log("and we are able to authenticate 1 ...");
log("no need to send cookie ... this one is good ...");
log(chess_.utype);
log(session);
                      const session_ = clone(session); 
                      delete session_.socketid;
                      session_.socketid = socket.socketid;
                      session_.utype = REGISTERED;
                      setSession(chess_.username, session_);
                      session_.code = 0;
                      session_.msg = "success!";
                      session_.restype = 'signin';
                      res.send({ registered: true, session: session_ } );
    log(SESSION_);
                      return;
                  } else { // possibly server issue
log("but unable to authenticate ... lost session in server ...");
log("we have to re-establish the cookie ...");

                      const profile = { sessionid: chess_.sessionid, unid: chess_.username };
                      const qres = signinPlayer(socket, profile, true);
                      qres.then((new_session) => {
                        new_session.restype = 'signin';
                        new_session.username = chess_.username;
                        new_session.socketid = socket.socketid;
                        new_session.utype = REGISTERED;
                        setSession(new_session.username, new_session);
                        res.cookie('chess_', JSON.stringify({ sessionid: new_session.sessionid, username: new_session.username, utype: REGISTERED }) );
                        res.send({ registered: true, session: new_session, utype: REGISTERED } );
                      });
    log(SESSION_);
                      return;

                  }
               } else {
log("yes ... we also have a session id ... and this is a guest user ...");
                     session = await authGuest(chess_.sessionid, chess_.username, true);
                     if (session != null) {
log("and we are able to authenticate 2 ...");
log("no need to send cookie except if it expired ... this one is good ...");
log(chess_.utype);
log(session);
                        if (session.expired) { // expired token
                          if (session.new_session) {
                              const new_session = session.new_session;
                              new_session.socketid = socket.socketid;
                              new_session.utype = GUEST;
                              setSession(new_session.username, new_session);
                              res.cookie('chess_', JSON.stringify({ sessionid: new_session.sessionid, username: new_session.username, utype: GUEST }) );
                              res.send({ token: new_session.sessionid, username: new_session.username, utype: GUEST } );
    log(SESSION_);
                              return;
                          }
                        } 
                        session.socketid = socket.socketid;
                        session.utype = GUEST;
                        res.cookie('chess_', JSON.stringify({ sessionid: chess_.sessionid, username: chess_.username, utype: GUEST }) );
                        res.send({ token: chess_.sessionid, username: chess_.username, utype: GUEST } );
    log(SESSION_);
                        return;
                     } else { // possibly server issue.
log("but unable to authenticate ... lost session in server ...");
log("we have to re-establish the cookie ...");
                        const new_session = createSession(GUEST);
                        new_session.username = chess_.username;
                        new_session.socketid = socket.socketid;
                        new_session.utype = GUEST;
                        setSession(new_session.username, new_session);
                        res.cookie('chess_', JSON.stringify({ sessionid: new_session.sessionid, username: new_session.username, utype: GUEST }) );
                        res.send({ token: new_session.sessionid, username: new_session.username, utype: GUEST } );
    log(SESSION_);
                        return;
                     }
               }
            }

            isOrphanedGame(chess_, game);
         }
    });
    
    
    router.post('/simul',  function(req, res) { // Let everyone, even guest, see (to invite more members )
         var profile = req.body;
log("list simul ...");
         const session = authSession(profile.sessionid, profile.username);
         if (session != null) {
             var simuls = getSimuls(profile);
log(simuls); 
             res.send({ code: 0, simul: simuls} );
         } else {
              res.send({ code: 10, msg: 'AuthError: Unable to authenticate ...'});
         }
         return true;
    });

    router.post('/tourneys',  function(req, res) { // Let everyone, even guest, see (to invite more members )
         var profile = req.body;
         const session = authSession(profile.sessionid, profile.username);
         if (session != null) {
               var resp = getTournaments(profile);
               resp.then((tourneys) => {
                   res.send(tourneys);
           
               })
         } else {
              res.send({ code: 10, msg: 'AuthError: Unable to authenticate ...'});
         }
         return true;
    
    });
    
    router.get('/play', function(req, res) {
         var token = req.query.token;
         jwt.verify(token, 'rmoo_welcome1', function(err, decode) {
            if (!err) {
               var secrets = {'accountNo' : '23423423', 'pin' : '2342342' };
               res.json(secrets);
            } else {
               res.send(err);
            }
         });
    });


    // Setup essential routes 
    router.get('/', function(req, res) { 
        res.header("Cross-Origin-Embedder-Policy", "require-corp");
        res.header("Cross-Origin-Opener-Policy", "same-origin");
        res.sendFile(path.join(__dirname + '/index.html')); 
    }); 
    
    //add the router 
    app.use('/', router); 
    const port = process.env.port || 3000;
    // app.listen(process.env.port || 3000); 
    console.log('Running at Port 3000'); 
    
    
    server.listen(port, () => {
            console.log('Server is up!');
    });
    
    /**************************************** END OF APP ROUTING ******************************************/
    
    /************************************* BEGIN OF SOCKET CALLS ******************************************/
    
    let broadcaster
    
    io.on('connection', function (socket) {

    /************************* Video Streaming ***************************/

      socket.on("broadcaster", () => {
        broadcaster = socket.id;
    log("broadcaster ....");
    log(socket.id);
        socket.broadcast.emit("broadcaster");
      });
      socket.on("watcher", () => {
    log("watcher  ....");
        socket.to(broadcaster).emit("watcher", socket.id);
        return true;
      });


      socket.on("disconnect", () => {
        socket.to(broadcaster).emit("disconnectPeer", socket.id);
        return true;
      });
    
      socket.on("offer", (id, message) => {
    log("offer  ....");
        socket.to(id).emit("offer", socket.id, message);
        return true;
      });
    
      socket.on("answer", (id, message) => {
    log("answer  ....");
        socket.to(id).emit("answer", socket.id, message);
        return true;
      });
    
      socket.on("candidate", (id, message) => {
        socket.to(id).emit("candidate", socket.id, message);
        return true;
      });

    /************************* End Video Streaming ***************************/
    
      socket.on('signup', function(profile) {
           console.log(profile); 
           const res = signupPlayer(profile);
           res.then((res) => {
               res.restype = 'signup';
               socket.emit('signedup', res); // send email confirmation
           })
           return true;
      });
    
        socket.on('signin', function(profile) {
            console.log(profile); 
            const res = signinPlayer(socket, profile);
            res.then((resp) => {
              resp.restype = 'signin';
              socket.emit('signedin', resp);
            })
            return true;
        });
    
        socket.on('signout', function(profile) {
            console.log(profile);
            const res = signoutPlayer(socket, profile);
            res.then((res) => {
              res.restype = 'signout';
              socket.emit('signedout', res);
            })
            return true;
        });
    
        socket.on('updateprofile', function(profile) {
    log(".....");
    log(profile);
    log(SESSION_);
            const session = authSession(profile.sessionid, profile.username);
    log(session);
            if (session != null) {
                 const res = updateProfile(socket, profile);
                 res.then((resp) => {
                     if (!resp.code) {
                         socket.emit('profileupdated', resp);
                     } else {
                         // socket.emit('profileupdated', resp);
                     }
                 });
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });
    
        socket.on('savetournament', function(tourn) {
    log(".....");
    log(tourn);
            const session = authSession(tourn.sessionid, tourn.username);
            if (session != null) {
                 const res = saveTournament(socket, tourn);
                 res.then((resp) => {
                     socket.emit('savedtournament', resp);
                 });
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });
    
        socket.on("resign", async function(profile) {
log("resigning ...");
log(profile);
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
                resign(socket, session, profile);
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
        });

        socket.on("abortgame", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
                abortGame(socket, session, profile);
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("acceptabort", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
                acceptAbort(socket, session, profile);
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("offerdraw", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
                offerDraw(socket, session, profile);
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("acceptdraw", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
                acceptDraw(socket, session, profile);
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("takeback", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
                takeback(socket, session, profile);
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("accepttakeback", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
                acceptTakeback(socket, session, profile);
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("acktournament", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
                await ackTournament(socket, session, profile);
            } else {
                socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("fetchgame", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
                 var game = fetchGame(profile);
                 game.then((resp) => {
log("game it ...");
log(resp);
                    socket.emit("showgame", { sessionid: session.sessionid, username: session.username, game: resp.game });
                 });
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
        });

        socket.on("watchtourney", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
log("so what??");
               log(profile);
var xx = getTourneys();
log(xx);
               var currentround = 0;
               var tourney = getTourney(profile.gametoken);
               if (tourney != null) {
                 status = tourney.status;
                 currentround = tourney.currentround;
               } 
               var tourney = watchTournament(profile.gametoken, profile.variant);
               tourney.then((resp) => {
log("good ...");
log(session);
                 resp.tourney.currentround = currentround;
                 socket.emit("watchtourney", { sessionid: session.sessionid, username: session.username, tourney: resp.tourney });
               });
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
        });

        socket.on("jointourney", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
//    log("Join Tourney human games ...");
//    log(profile);
                if (profile.utype == GUEST) {
                      socket.emit("joinedtourney", { code: 11, msg: "Guest not allowed to join tournaments!" });
                      return;
                }
                res = joinTournament(socket, session, profile);
                res.then((resp) => {
                    socket.emit("joinedtourney", resp);
                });
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("leavetourney", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
//    log("Leave Tourney human games ...");
//    log(profile);
                if (profile.utype == GUEST) {
                      socket.emit("lefttourney", { code: 11, msg: "Guest left the tournaments!" });
                      return;
                }
                res = leaveTournament(socket, session, profile);
                res.then((resp) => {
                    socket.emit("lefttourney", { code: resp.code, msg: resp.msg });
                });
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("joinsimul", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
    log("Join Simultaneous human games ...");
    log(profile);
                joinSimul(socket, session, profile);
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("cancelsimul", async function(profile) {
            const session = authSession(profile.sessionid, profile.username);
            if (session != null) {
                cancelSimul(socket, session, profile);
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });
    
        socket.on("simul", async function(match) {
            const session = authSession(match.sessionid, match.username);
            if (session != null) {
    log("Simultaneous human games ...");
    log(match);
                simulTournaments(socket, session, match);
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });
    
        socket.on("match", async function(match) {
            if (match.utype != null && match.utype == GUEST) {
    log("match from guests ...");
    log(match);
              const session = await authGuest(match.sessionid, match.username);
    log(session);
              if (session != null) {
                  if (match.rematch) { // check first if we already have a previous match before popping out a new opponent
log("ok now ...");
                     rematchGame(socket, session, match);
                  } else {
    	             matchGame(socket, session, match);
                  }
              } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
              }
            } else {
              const session = authSession(match.sessionid, match.username);
              if (session != null) {
                  if (match.rematch) { // check first if we already have a previous match before popping out a new opponent
log("ok now ...");
                     rematchGame(socket, session, match);
                  } else {
    	             matchGame(socket, session, match);
                  } 
              } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
              }
            }
            return true;
        });
    
        socket.on("move", function(turn) {
    log("move detected ...");
    log(turn);
            const session = authSession(turn.sessionid, turn.username);
            if (session != null) {
               if (turn.tourney) {
                    const tourney    = getTourney(turn.gametoken); 
log("we detect a move ...");
log(tourney);
                    const games   = tourney.pairing.plan[turn.round];
                    const seat    = tourney.players;
                    const game    = { gameid: null, move: turn.move, simul: false, tourney: true, dropped: turn.dropped } // dropped for Crazy House
                    var psocketid = null;
                    for (var g in games) {
                        var game_ = games[g];
                        var white = seat[game_.white];
                        var black = seat[game_.black];
                        if (white != null && black != null) {
                              
                              if (white.player.username == turn.username) {

                                  white.missing = 0; // player is actively playing, thus don't have to forfeit

                                  const bsession = getSession(black.player.username);
                                  game.side      = WHITE;
                                  game.player    = white.player.username; /* the player who moved */
                                  game.username  = black.player.username;
                                  game.gameid    = game_.bgameid; /* gameboard of opponent */
                                  if (bsession != null) {
                                    game.sessionid = bsession.sessionid;
                                    psocketid      = bsession.socketid;
                                  }
                              } else 
                              if (black.player.username == turn.username) {

                                  black.missing = 0; // player is actively playing, thus don't have to forfeit

                                  const wsession = getSession(white.player.username);
                                  game.side      = BLACK;
                                  game.player    = black.player.username; /* the player who moved */
                                  game.username  = white.player.username;
                                  game.gameid    = game_.wgameid; /* gameboard of opponent */
                                  if (wsession != null) {
                                    game.sessionid = wsession.sessionid;
                                    psocketid      = wsession.socketid;
                                  }
                              }
                              if (psocketid != null) {
                                 if (game_.moves == null) game_.moves = [];
                                 if (game_.elapsed == null) game_.elapsed = [];
                                 game_.moves.push(turn.move);
                                 game_.elapsed.push(turn.elapsed);
                                 setTourney(tourney.tournament_id, tourney); /* send it back for update in the distributed system */
                                 socket.broadcast.to(psocketid).emit('move', game ); // for the others.
                                 break;
                              }
                        }
                    }

               } else
               if (turn.simul) {
    log("simul ...");
                    const simul    = getSimul(turn.gametoken); // Simul[turn.gametoken];
                    const splayers = getSimulPlayers(turn.gametoken); // Simul_Players[turn.gametoken];
    log(simul);
                    const game = { gameid: null, move: turn.move, simul: true, dropped: turn.dropped } // dropped for Crazy House
                    var psocketid = null;
                    if (simul != null) {
                        if (turn.username == simul.host) {
                            const rplayer  = splayers.players[turn.remoteplayer];
                            game.side      = simul.host_color;
                            game.player    = simul.host; /* the player who moved */
                            game.username  = rplayer.player;
                            game.host      = false;
                            game.sessionid = getSession(rplayer.player).sessionid; // SESSION_[rplayer.player].sessionid;
                            game.gameid    = rplayer.gameid; /* gameboard of opponent */
                            psocketid      = rplayer.socketid;
                        } else {
                            const host     = splayers.host;
                            const rplayer  = splayers.players[turn.username];
                            game.side      = rplayer.color;
                            game.player    = rplayer.player; /* the player who moved */
                            game.username  = simul.host;
                            game.host      = true;
                            game.gameid    = rplayer.host_gameid; /* gameboard of opponent */
                            game.sessionid = getSession(simul.host).sessionid; // SESSION_[simul.host].sessionid;
                            psocketid      = host.socketid;
                        }
                        simul.moves.push(turn.move);
                        simul.elapsed.push(turn.elapsed);
                        setSimul(simul.gametoken, simul); /* send it back for update in the distributed system */
                        socket.broadcast.to(psocketid).emit('move', game ); // for the others.
                    }
               } else {
log("not simul");
                    const challenge = getMatch(turn.gametoken); 
                    const game = { gameid: null, move: turn.move, simul: false, dropped: turn.dropped } // dropped for Crazy House
                    var psocketid = null;
                    if (challenge != null) {
                        if (turn.username == challenge.wgame.player) {
                            game.side      = WHITE;
                            game.player    = challenge.wgame.player; /* the player who moved */
                            game.username  = challenge.bgame.player;
                            game.sessionid = getSession(challenge.bgame.player).sessionid; // SESSION_[challenge.bgame.player].sessionid;
                            game.gameid    = challenge.bgame.gameid; /* gameboard of opponent */
                            psocketid      = challenge.bgame.socketid;
                        } else
                        if (turn.username == challenge.bgame.player) {
                            game.side   = BLACK;
                            game.player = challenge.bgame.player; /* the player who moved */
                            game.username = challenge.wgame.player;
                            game.sessionid = getSession(challenge.wgame.player).sessionid; // SESSION_[challenge.wgame.player].sessionid;
                            game.gameid = challenge.wgame.gameid; /* gameboard of opponent */
                            psocketid = challenge.wgame.socketid;
                        }
                        challenge.moves.push(turn.move);
                        challenge.elapsed.push(turn.elapsed);
                        setMatch(challenge.gametoken, challenge); /* send it back for update in the distributed system */
                        socket.broadcast.to(psocketid).emit('move', game ); // for the others.
                     }
                }
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });

        socket.on("revealmole", async function(user) {
            const session = authSession(user.sessionid, user.username);
            if (session != null) {
               var game = user.game;
               if (game != null) {
log("reveal Mole ...");
log(user);
                  revealMole(socket, user);
               }
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });
    
        socket.on("recordgame", async function(data) {
            const user = data.session;
            const session = authSession(user.sessionid, user.username);
    log("recording game  ...");
    log(data);
            if (session != null) {
    log("session in ...");
              const game = data.game;
    log(game);
              if (game.simul) {
                  await recordSimul(socket, user, game);
              } else
              if (game.tourney) {
                  await recordTourney(socket, user, game);
              } else {
                  await recordMatch(socket, user, game);
              }
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });
    
        socket.on("changeratesystem", function (rating) {
            const auth = authSession(rating.sessionid, rating.username);
            if (auth != null) {
                   CONFIG_[rating.username].ratesystem = rating.ratesystem;
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });
    
        socket.on("saveconfig", function (data) {
            const auth = authSession(data.sessionid, data.username);
            if (auth != null) {
                   CONFIG_[data.username] = data.config;
                   const res = saveConfig(socket, data);
                   res.then((res) => {
                      if (res != null) {
                         if (res.code == 0) {
                            socket.emit('savedconfig', { code: 0, msg: 'Success!' });
                         }
                      }
                   });
            } else {
                 socket.emit('AuthError', 'Unable to authenticate ...');
            }
            return true;
        });
    
        socket.on('disconnect', function (res) {
            console.log("");
            console.log('disconnected ...');
            console.log(res);
            return true;
        });
    
        socket.on('disconnecting', function (res) {
            console.log('disconnecting ...');
            console.log(socket.id);
            console.log(res);
            return true;
        });

        socket.on('pong', function (res) {
            session = authSession(res.sessionid, res.userid);
            if (session != null) {
               var latency = (Date.now() - session.latency_start);
log("latency of " + res.userid + " is " + latency);
            }
        });

    });
    
     function checkLatency(socket, socketid, sessionid, userid) {
        session = authSession(sessionid, userid);
log("session for latency ...");
log(sessionid);
log(userid);
log(session);
        if (session != null) {
            session.latency = 0;
            session.latency_start = Date.now();
            socket.broadcast.to(socketid).emit('ping', { sessionid: session.sessionid, userid: session.username } ); 
        }
     }
     
    
    /******************************************* END OF SOCKET CALLS ******************************************/
    
    /******************************************* BEGIN APPLICATION FUNCTIONS  *********************************/
    
    const wINITFEN_ = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQKq - 0 1";
    const wINITFENHORDE_ = "rnbqkbnr/pppppppp/8/1PP2PP1/PPPPPPPP/PPPPPPPP/PPPPPPPP/PPPPPPPP w kq - 0 1";
    
    // Variants
    const STANDARD    = 'Standard';
    const CHESS960    = 'Chess960';
    const CRAZYHOUSE  = 'Crazy_House';
    const KINGOFTHEHILL  = 'King_Of_The_Hill';
    const THREECHECKS = 'Three_Checks';
    const ATOMIC      = 'Atomic';
    const HORDE       = 'Horde';
    const THEMOLE     = 'The Mole';
    const THETRAITOR  = 'The Traitor';
    const ANTICHESS = 'AntiChess';
    const variants = [
                        { code: '', item: 'Standard', initial: 'Std' },
                        { code: '', item: 'Chess960', initial: 'c960' },
                        { code: '', item: 'Crazy House', initial: 'crazyh' },
                        { code: '', item: 'King of the Hill', initial: 'kinghill' },
                        { code: '', item: 'Three Checks', initial: 'threechecks' },
                        { code: '', item: 'Atomic', initial: 'atomic' },
                        { code: '', item: 'Horde', initial: 'horde' },
                        { code: '', item: 'The Mole', initial: 'mole' },
                        { code: '', item: 'The Traitor', initial: 'traitor' },
                        { code: '', item: 'AntiChess', initial: 'antichess' }
                    ];
    
    function scaleF(rating, n) {
       if (!n) return 40;
       if (n && rating < 2400) return 20;
       if (n && rating >= 2400) return 10;
    }
    
    function scoreP(result, c = WHITE) {
        if ((c == WHITE && result == '1-0') || (c == BLACK && result == '0-1')) return 1; 
        if ((c == WHITE && result == '0-1') || (c == BLACK && result == '1-0')) return 0; 
        return 0.5;
    }
    
    function fullName(id, file, mtype) {
      if (mtype != null) {
        oldext = mtype.match(/^image\/(svg|svg\+xml|png|gif||jpg|jpeg)$/i)[1];
        return id + '_' + file + '.' + oldext.replace("+xml","");
      } else {
        return null;
      }
    }
    
    function randomFENchess960() {
           const position = { a:'',b:'',c:'',d:'',e:'',f:'',g:'',h:'' };
           // generate random position of bishops
           const B1 = ['a','c','e','g'], bB = Math.round(Math.random() * 3);
           const B2 = ['b','d','f','h'], wB = Math.round(Math.random() * 3);
           const b1 = B1[wB], b2 = B2[bB];
           position[b1] = 'B'; position[b2] = 'B';
           B1[wB] = ''; B2[bB] = '';
    
           // generate random position of queen using B1 and B2 arrays
           const Q1 = B1.concat(B2).join('').split(''), Q = Math.round(Math.random() * 5);
           const q1 = Q1[Q];
           position[q1] = 'Q';
           Q1[Q] = '';
    
           // generate random position of the first knights using Q1 array
           const N1 = Q1.join('').split(''), Na = Math.round(Math.random() * 4);
           const n1 = N1[Na];
           position[n1] = 'N';
           N1[Na] = '';
    
           // generate random position of the first knights using N1 array
           const N2 = N1.join('').split(''), Nb = Math.round(Math.random() * 3);
           const n2 = N2[Nb];
           position[n2] = 'N';
           N2[Nb] = '';
    
           // generate random position of the rooks and Queens using N2 array
           const KR = N2.join('').split('').sort();
           position[KR[0]] = 'R';
           position[KR[1]] = 'K';
           position[KR[2]] = 'R';
    
           var parr = [];
           for (var p in position) { parr.push(position[p]); }
    
           var wrank = parr.join(''), brank = wrank.toLowerCase();
    
           const fen_ = brank + "/pppppppp/8/8/8/8/PPPPPPPP/" + wrank + " w KQKq - 0 1";
    
           return fen_;
    }

    const fetchGame = async(profile) => {
      var tourney = profile.tourney;
      var PDB_ = null;
log("show game ...");
log(tourney);
      try {
             const PDB_ = await client.connect();
             const records  = await PDB_.query("SELECT T.*, WP.username AS wusername, BP.username AS busername, " + 
                      " WP.fide_title AS wtitle, BP.fide_title AS btitle FROM chessdb.games T, chessdb.players WP, chessdb.players BP WHERE " +
                      " T.tournament_id = $1 and T.round = $2 and T.white_id = $3 and T.black_id = $4 and " + 
                      " T.white_id = WP.player_id and T.black_id = BP.player_id",
                      [ tourney.pair.tournament_id, tourney.pair.round, tourney.pair.wplayer_id, tourney.pair.bplayer_id ]);
             var game = null, ratings = null;
log("how many games???");
log(records.rowCount);
             if (records.rowCount > 0) {
                 game = records.rows[0];

                 ratings = await PDB_.query( "SELECT R.variant, R.timer_type, R.elo, R.glicko, R.glicko_d, R.glicko_v, R.rmoo " +
                               " FROM chessdb.ratings R WHERE R.player_id = $1 and R.variant = $2", 
                                  [ records.rows[0].white_id, records.rows[0].variant ] );
                 if (ratings.rowCount > 0) { game.wrating = ratings.rows; }

                 ratings = await PDB_.query( "SELECT R.variant, R.timer_type, R.elo, R.glicko, R.glicko_d, R.glicko_v, R.rmoo " +
                               " FROM chessdb.ratings R WHERE R.player_id = $1 and R.variant = $2", 
                                 [ records.rows[0].black_id, records.rows[0].variant ] );
                 if (ratings.rowCount > 0) { game.brating = ratings.rows; }

             }
             PDB_.release();
             return { code: 0, msg: 'success!', game: game }
          } catch (error) {
             console.log(error);
log("where is the error ...");
log(error.where);
             if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid, where: error.where }
              console.log(res)
              return res;
          } finally {
         //   await client.end()
          }
    }

    const watchTournament = async(gametoken, variant) => {
      var PDB_ = null;
      var ndays = 24 * 60 * 60 * 1000;
      var ntoday = UTCtoday() - ( 7 * ndays );
      var mtoday = UTCtoday() + ( 30 * ndays );
      var today = UTCtoday();
      var lastweek = (new Date(ntoday)) / 1;
      var onemonth = (new Date(mtoday)) / 1;
log("variant ...");
log(variant);
      try {
             const PDB_ = await client.connect();
             const records  = await PDB_.query("SELECT P.*, count(T.tournament_id) as joinedplayers FROM ( SELECT P.username AS hosted, " +
                                " T.tournament_id, T.tournament_name, T.tournament_type, T.duration, T.number_players, T.variant, " +
                                " T.timer, T.increment, T.rated, T.titled, T.start_date, T.end_date, T.forfeit_policy, T.status, T.currentround " +
                                " FROM  chessdb.players P, chessdb.tournaments T " +
                                " WHERE T.host_id = P.player_id  and T.tournament_id = $1) P" +
                                " LEFT OUTER JOIN chessdb.tournament_registration T ON " +
                                " P.tournament_id = T.tournament_id group by P.hosted, P.tournament_id, " +
                                " P.tournament_name, P.tournament_type, P.duration, P.number_players, P.variant, P.timer, P.increment, " +
                                " P.rated, P.titled, P.start_date, P.end_date, P.forfeit_policy, P.status, P.currentround, T.tournament_id ORDER by P.start_date asc",
                                [ gametoken ]);
             var tourney = null;
             if (records.rowCount > 0) {
                tourney = records.rows[0];
                const precord = await PDB_.query(
                                "SELECT T.*, WP.username AS wusername, BP.username AS busername, WP.fide_title AS wtitle, BP.fide_title as btitle " + 
                                " FROM chessdb.tournament_pairing T, " + 
                                " chessdb.players WP, chessdb.players BP " + 
                                " WHERE T.tournament_id = $1 and T.wplayer_id = WP.player_id and T.bplayer_id = BP.player_id ORDER by " +
				" T.tournament_id, T.round ", [gametoken]);
log("pairing ...");
log(precord.rows);
                if (precord.rowCount > 0) {
                   tourney.pairing = precord.rows;
                   tourney.ratings = {};
                   var nratings = {};
                   for (var p in tourney.pairing) {
                      var pair = tourney.pairing[p];
                      nratings[pair.wusername] = { rating: null, player_id: pair.wplayer_id };
                      nratings[pair.busername] = { rating: null, player_id: pair.bplayer_id };
                   }
                   for (var p in nratings) {
                      var player = nratings[p];
                      const ratings = await PDB_.query( "SELECT R.variant, R.timer_type, R.elo, R.glicko, R.glicko_d, R.glicko_v, R.rmoo " +
                               " FROM chessdb.ratings R WHERE R.player_id = $1 and R.variant = $2", [ player.player_id, variant ] );
                      if (ratings.rowCount > 0) {
                         tourney.ratings[p] = ratings.rows;
                      }
                   }
                }
             }

log("one tourney coming up ...");
log(tourney);
             PDB_.release();
             return { code: 0, msg: 'success!', tourney: tourney }
          } catch (error) {
             console.log(error);
log("where is the error ...");
log(error.where);
             if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid, where: error.where }
              console.log(res)
              return res;
          } finally {
         //   await client.end()
          }
    }

    const getTournaments = async(profile, system = false) => {
      var PDB_ = null;
      var ndays = 24 * 60 * 60 * 1000;
      var ntoday = UTCtoday() - ( 30 * ndays );
      var mtoday = UTCtoday() + ( 30 * ndays );
      var today = UTCtoday(); 
      var lastweek = (new Date(ntoday)) / 1;
      var onemonth = (new Date(mtoday)) / 1;
      try {
             const PDB_ = await client.connect();
             const filter_by_status = (system) ? "and T.status <> 3" : "";
             const records  = await PDB_.query("SELECT P.*, count(T.tournament_id) as joinedplayers FROM ( SELECT P.username AS hosted, " +
                                " T.tournament_id, T.tournament_name, T.tournament_type, T.duration, T.number_players, T.variant, " + 
                                " T.timer, T.increment, T.rated, T.titled, T.start_date, T.end_date, T.forfeit_policy, T.status, T.currentround " +
                                " FROM  chessdb.players P, chessdb.tournaments T " + 
                                " WHERE T.host_id = P.player_id " + filter_by_status + " and T.start_date between to_timestamp($1) and to_timestamp($2)) P" +
				" LEFT OUTER JOIN chessdb.tournament_registration T ON " +
                                " P.tournament_id = T.tournament_id group by P.hosted, P.tournament_id, " +
                                " P.tournament_name, P.tournament_type, P.duration, P.number_players, P.variant, P.timer, P.increment, " +
                                " P.rated, P.titled, P.start_date, P.end_date, P.forfeit_policy, P.status, P.currentround, T.tournament_id ORDER by P.start_date asc",
                                [ lastweek/1000, onemonth/1000 ]);
             var tourneys = null;
             if (records.rowCount > 0) {
                tourneys = records.rows;
                if (!system) {
                    const precord = await PDB_.query(
                                "SELECT P.player_id FROM chessdb.players P WHERE P.username = $1", [profile.username]); 
                    if (precord.rowCount > 0) 
                    for (var p in tourneys) {
                       const tourney = tourneys[p];
                       tourney.player_status = 0; // used by clients to display status of a player accessing the tournament details.
                       tourney.simul = 0;
                       const record = await PDB_.query(
                            "SELECT T.tournament_id, T.status FROM chessdb.tournament_registration T WHERE T.tournament_id = $1 and T.player_id = $2",
                            [tourney.tournament_id, precord.rows[0].player_id]); 
                       if (record.rowCount > 0) { tourney.player_status = record.rows[0].status; }
                   }
                }
             }
             PDB_.release();
             return { code: 0, msg: 'success!', tourneys: tourneys }
          } catch (error) {
             console.log(error);
log("where is the error ...");
log(error.where);
             if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid, where: error.where }
              console.log(res)
              return res;
          } finally {
         //   await client.end()
          }
    }
    
    const saveTournament = async (socket, tourn) => {
      var PDB_ = null;
      try {
             const PDB_ = await client.connect();
    log("Saving Tournament!");
             const player   = await PDB_.query("SELECT P.player_id FROM chessdb.players P WHERE P.username = $1", [ tourn.username ]);
             const tourney  = await PDB_.query("SELECT P.tournament_id, P.status FROM chessdb.tournaments P WHERE P.host_id = $1 and lower(tournament_name) = lower($2)",
                                [ player.rows[0].player_id, tourn.name ]);
             if (tourney.rowCount == 0) {
    log("here inserting ...");
                const query = "INSERT INTO chessdb.tournaments(tournament_id, host_id, tournament_name, tournament_type, " + 
                      "duration, number_players, variant, timer, increment, " +
                      "rated, titled, start_date, recurrence, forfeit_policy, description, status, currentround, final_attendance) " +
                      "VALUES(nextval('chessdb.tournament_seq'), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, to_timestamp($11), $12, $13, $14, $15, $16, $17)";
                const qres = await PDB_.query( query, [player.rows[0].player_id, tourn.name, tourn.type, tourn.duration, tourn.players, tourn.variant,
                      tourn.timer, tourn.increment, tourn.rated, tourn.titled, tourn.start_date / 1000, tourn.recurrence, tourn.forfeit_policy,
                       tourn.description, 0, 0, 0 ]);
             } else {
    log("here updating ...");
log(tourn);

                // check that the record in DB has not started yet.
                if (parseInt(tourney.rows[0].status) == 0) {
                   const query = "UPDATE chessdb.tournaments set tournament_name = $1, tournament_type = $2, duration = $3, " + 
		      "number_players = $4, variant = $5, timer = $6, increment = $7, " +
                      " rated = $8, titled = $9, start_date = to_timestamp($10), recurrence = $11, forfeit_policy = $12, description = $13, status = $14, " + 
                      " currentround = $15, final_attendance = $16  WHERE tournament_id = $16 and host_id = $17 ";
                   await PDB_.query(query, [ tourn.name, tourn.type, tourn.duration, tourn.players, tourn.variant, tourn.timer, tourn.increment, tourn.rated, 
                      tourn.titled, tourn.start_date / 1000, tourn.recurrence, tourn.forfeit_policy, tourn.description, tourn.status, tourn.currentround,
                      tourn.attendance, tourney.rows[0].tournament_id, player.rows[0].player_id ]);
                } else {
                   PDB_.release();
                   return { code: 2, msg: 'tournament is ongoing.' }
                }
             }
             PDB_.release();
             return { code: 0, msg: 'success!' }
          } catch (error) {
             console.log(error);
log("where is the error ...");
log(error.where);
             if (PDB_ != null) PDB_.release();
             var res = null;
             if (error.code == 23505) {
                res = { code: error.code, msg: 'Tournament of the same time already exists', errid: error.errid, where: error.where };
             } else {
                res = { code: error.code, msg: error.msg, errid: error.errid, where: error.where };
             }
             console.log(res)
             return res;
          } finally { }
    }

    async function getPlayerSocket(socketid) {
      const sockets = await io.fetchSockets();
      for (var p in sockets) {
           var socket = sockets[p];
           if (socketid == socket.id) {
               return socket;
           }
      }
      return null;
    }

    async function forfeitPlayer(who_) {
      var PDB_ = null;
      try {
             const PDB_ = await client.connect();
             const query = "UPDATE chessdb.tournament_registration SET status = 4, reason = $1, updated = to_timestamp($2) WHERE tournament_id = $3 AND player_id = $4";
             await PDB_.query(query, [ "forfeited", UTCtoday()/1000, who_.player.tournament_id, who_.player.player_id ]);
             PDB_.release();
             return { code: 0, msg: 'success!' }
          } catch (error) {
             if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid, where: error.where }
              console.log(res)
              return res;
          } finally { }
    }

    async function updatePairing(tourney) {
          var code = 0, msg = "success";
log("updating pairing ...");
          try {
             const PDB_ = await client.connect();
             // await PDB_.query('BEGIN')
             var insquery = "INSERT INTO chessdb.tournament_pairing (tournament_id, round, wplayer_id, bplayer_id, result, " +
                         "wforfeit, bforfeit, moves, elapsed) " +
                         "VALUES ($1, $2, $3, $4, $5, to_timestamp($6), to_timestamp($7), $8, $9)";
             var updquery = "UPDATE chessdb.tournament_pairing set result = $1, wforfeit = to_timestamp($2), bforfeit = to_timestamp($3), " +
                         " moves = $4, elapsed = $5 " +
                         " WHERE tournament_id = $6 and round = $7 and wplayer_id = $8 and bplayer_id = $9";

             var plan = tourney.pairing.plan;
log("plan");
log(plan);
             var seat = tourney.players;
             for (var p in plan) {
                 var games = plan[p];
log("games");
log(games);
                 for (var g in games) {
                     var game = games[g];
                     var white = seat[game.white];
                     var black = seat[game.black];
                     var wforfeit = (white.forfeitround[p] != null) ? white.forfeitround[p] / 1000 : null;
                     var bforfeit = (black.forfeitround[p] != null) ? black.forfeitround[p] / 1000 : null;
                     var result = (white.result[p] == 1) ? '1-0' : '0-1';
                     if (white.forfeitround[p] != null) result = '0-1';
                     if (black.forfeitround[p] != null) result = '1-0';
                     if (white.result[p] == 0.50) result = '1/2-1/2';
                     if (!game.gameover) result = '';
                     try {
                         await PDB_.query(insquery, [ tourney.tournament_id, p, white.player.player_id, black.player.player_id,
                                  result, wforfeit, bforfeit, game.moves.join(','), game.elapsed.join(',') ]);
                     } catch(err) {
                         if (err.code == 23505) {
                           await PDB_.query(updquery, [ result, wforfeit, bforfeit, game.moves.join(','), game.elapsed.join(','),
                                                      tourney.tournament_id, p, white.player.player_id, black.player.player_id ]);
                         } 
                     }
                 }

             } 
             PDB_.release();
             // PDB_.query('COMMIT')
          } catch (error) {
log("error ...");
log(error);
            if (PDB_ != null) PDB_.release();
            // PDB_.query('ROLLBACK')
            code = error.code; msg: error.msg;
          } finally { 
            // PDB_.release();
          } 
          return { code: code, msg: msg }
    } 

    const endTournament = async (tourney) => {
          var PDB_ = null;
          try {
             const PDB_ = await client.connect();
    log("end Tournament!");
    log(tourney);
             tourney.status = 3;
             var today = UTCtoday();
             var query = "UPDATE chessdb.tournaments set status = $1, final_attendance = $2, end_date = to_timestamp($3) where tournament_id = $4 ";
             await PDB_.query(query, [ tourney.status, tourney.attendance, today/1000, tourney.tournament_id ]);
             tourney.end_date = today;
             PDB_.release();

             return { code: 0, msg: 'success!' }
          } catch (error) {
             console.log(error);
             if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid, where: error.where }
              console.log(res)
              return res;
          } finally { }
    }
  
    const startTournament = async (tourney) => {
          var PDB_ = null;
          try {
             const PDB_ = await client.connect();
    log("start Tournament!");

             tourney.status = 1;
             var today = UTCtoday();
             var query = "UPDATE chessdb.tournaments set status = $1, start_date = to_timestamp($2) where tournament_id = $3 ";
             await PDB_.query(query, [ tourney.status, today/1000, tourney.tournament_id ]);
             tourney.start_date = today;
             PDB_.release();

             return { code: 0, msg: 'success!' }
          } catch (error) {
             console.log(error);
             if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid, where: error.where }
              console.log(res)
              return res;
          } finally { }
    }

    async function getOnlinePlayers(tourney) {
          /* Check if players are online */
          for (var p in tourney.players) {
               var who_ = tourney.players[p];
               var session = getSession(who_.player.username);
               if (session != null) {
                   if (!who_.forfeited) {
                      const socket = await getPlayerSocket(session.socketid);
                      who_.socket = socket;
                      who_.missing = 0;
                      who_.forfeited = false;
                   }
               } else {
                  // if player does not make his move over the forfeit policy, player forfeits from the game only
                  // and not from the tournament. this gives the player a chance to come back to the tournament
                  // in case the player has bad network connection.
log("forfeiting ...");
log(tourney.currentround);
log(who_);
                  if ((who_.missing * SCHED_TIMER) / 1000 >= tourney.forfeit_policy) {
                     var len = 0;
                     who_.socket = null;
                     who_.forfeited = true;
                     who_.forfeitround[tourney.currentround] = UTCtoday();
                     len = Object.keys(who_.forfeitround).length;
                      // if player is forfeited in two games, then player is out of the tournament.
                     if (len == 2)  forfeitPlayer(who_); 
                  }
                  who_.missing ++;  /* this line is below the condition above, to give players warm up time */
               }
          }
    }

    async function getRegisteredPlayers(tourney) {
        var PDB_ = null;
        try {
             const PDB_ = await client.connect();
             /* get all valid registered players (1 - joined, 4 - playing - 6 completed */
             query = "SELECT P.username, P.player_id, T.tournament_id FROM (SELECT T.player_id, T.tournament_id FROM chessdb.tournament_registration T " +
                     "WHERE T.tournament_id = $1) T, " +
                     "chessdb.players P where T.player_id = P.player_id";

             const players = await PDB_.query(query, [ tourney.tournament_id ]);
             const online = []; 

             if (players.rowCount > 0) {
                 for (var p in players.rows) {
                    var player = players.rows[p];
                    // collect registered players.
                    online.push({socket: null, player: player, score: 0, result: [], missing: 0, forfeited: false, forfeitround: {} }); 
                 }
             }
             PDB_.release();
             return { code: 0, msg: 'success!', players: online }
          } catch (error) {
             console.log(error);
             if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid, where: error.where }
              console.log(res)
              return res;
          } finally { }
    }

    const joinTournament = async (socket, session, profile) => {
      var PDB_ = null;
      try {
             const PDB_ = await client.connect();
    log("Joining Tournament!");
log(profile);
             const player   = await PDB_.query("SELECT P.player_id FROM chessdb.players P WHERE P.username = $1", [ profile.username ]);
             const query    = "INSERT INTO chessdb.tournament_registration(tournament_id, player_id, joined, status, reason ) " +
                              "VALUES($1, $2, to_timestamp($3), $4, $5)";
             if (player.rowCount > 0) {
                 const qres = await PDB_.query( query, [profile.gametoken, player.rows[0].player_id, UTCtoday() / 1000, profile.status, "joined" ]);
             } else throw { code: 10, msg: "error!" }; 

             PDB_.release();
             return { code: 0, msg: 'Player successfully registered!', joined_date: UTCtoday()  };
          } catch (error) {
             var res = null;
             console.log(error);
             if (PDB_ != null) PDB_.release();
             if (error.code == 23505) {
               res = { code: error.code, msg: 'Player already registered!' };
             } else {
               res = { code: error.code, msg: 'Server error!', errid: error.errid, where: error.where };
             }
               console.log(res)
             return res;
          } finally { }
    }

    const leaveTournament = async (socket, session, profile) => {
      var PDB_ = null;
      try {
             const PDB_ = await client.connect();
    log("Joining Tournament!");
log(profile);
             const player   = await PDB_.query("SELECT player_id FROM chessdb.players  WHERE username = $1", [ profile.username ]);
             if (player.rowCount > 0) {
                 const regis   = await PDB_.query("DELETE FROM chessdb.tournament_registration WHERE player_id = $1", [ player.rows[0].player_id ]);
             } else throw { code: 10, msg: "error!" };
             PDB_.release();
             return { code: 0, msg: 'Player successfully de-registered!' }
          } catch (error) {
             var res = null;
             console.log(error);
             if (PDB_ != null) PDB_.release();
             res = { code: error.code, msg: 'Server error!', errid: error.errid, where: error.where };
             console.log(res)
             return res;
          } finally { }
    }
    
    const saveConfig = async (socket, data) => {
      var PDB_ = null;
      try {
             const PDB_ = await client.connect();
    log("Updating Config!");
             const player = await PDB_.query("SELECT P.player_id FROM chessdb.players P WHERE P.username = $1", [ data.username ]);
             const profile = await PDB_.query("SELECT P.profile_id FROM chessdb.profiles P WHERE P.profile_id = $1", [ player.rows[0].player_id ]);
             if (profile.rowCount == 0) {
                const query = "INSERT INTO chessdb.profiles(profile_id, config) VALUES($1, $2)";
                const qres = await PDB_.query( query, [ player.rows[0].player_id, data.config ]);
             } else {
                await PDB_.query("UPDATE chessdb.profiles set config = $1 where profile_id = $2 ", [ data.config, profile.rows[0].profile_id ]);
             }
             PDB_.release();
             return { code: 0, msg: 'success!' }
          } catch (error) {
             console.log(error);
             if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid }
              console.log(res)
              return res;
          } finally {
         //   await client.end()
          }
    }
    
    const savePhoto = async (profile, files, photo_dir) => {
        var PDB_ = null, profiletb;
        var ext = files.file.originalFilename.match(/(\.svg|\.png|\.gif|\.jpg|\.jpeg)$/i)[0];
        try {
             const PDB_ = await client.connect();
             const player = await PDB_.query("SELECT P.player_id FROM chessdb.players P WHERE P.username = $1", [ profile.username ]);
             profiletb    = await PDB_.query("SELECT P.profile_id, P.imagefile, P.mimetype FROM chessdb.profiles P WHERE P.profile_id = $1", [ player.rows[0].player_id ]); 
             if (profiletb.rowCount == 0) {
                const query = "INSERT INTO chessdb.profiles(profile_id, imagefile, filename, mimetype, filesize) VALUES($1, $2, $3, $4, $5) returning imagefile";
                const qres  = await PDB_.query( query, [ player.rows[0].player_id, files.file.newFilename, files.file.originalFilename, files.file.mimetype, files.file.size ]);
    
                const newfile = photo_dir + "/" + files.file.newFilename,
                  renamedfile = photo_dir + "/" + player.rows[0].player_id + "_" + files.file.newFilename + ext;
                const ren = await fs.rename( newfile, renamedfile,(err) => {
                    if (err) { return err };
                    return null;
                 });
                 if (ren != null)  return { code: 1125, msg: 'Photo failed to upload!' }
                 return { code: 0, msg: 'Photo uploaded!', image: player.rows[0].player_id + "_" + files.file.newFilename + ext}
    
             } else {
               // await PDB_.query("UPDATE chessdb.profiles set config = $1 where profile_id = $2 ", [ data.config, profiletb.rows[0].profile_id ]);
               const query = "UPDATE chessdb.profiles set imagefile = $1,  filename = $2, mimetype = $3, filesize = $4 where profile_id = $5";
               const qres = await PDB_.query(query,
                       [ files.file.newFilename, files.file.originalFilename, files.file.mimetype, files.file.size, profiletb.rows[0].profile_id ]);
    
               // delete old file and rename new file.
                const oldfile = photo_dir + "/" + fullName(profiletb.rows[0].profile_id, profiletb.rows[0].imagefile, profiletb.rows[0].mimetype);
                      newfile = photo_dir + "/" + files.file.newFilename,
                  renamedfile = photo_dir + "/" + profiletb.rows[0].profile_id + "_" + files.file.newFilename + ext;
                log("unlinking: " + oldfile);
    
                const unl = await fs.unlink( oldfile, (err) => {
                      if (err) { return err };
                      return null;
                } )
                if (unl != null)  return { code: 1124, msg: 'Photo failed to upload!' }
                const ren = await fs.rename( newfile, renamedfile,(err) => {
                      if (err) { return err };
                      return null;
                } )
                if (ren != null)  return { code: 1124, msg: 'Photo failed to upload!' }
                return { code: 0, msg: 'Photo uploaded!', image: profiletb.rows[0].profile_id + "_" + files.file.newFilename + ext }
             }
             PDB_.release();
             return { code: 0, msg: 'success!' }
          } catch (error) {
             console.log(error);
             if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid }
              console.log(res)
              return res;
          } finally {
         //   await client.end()
          }
    }
    
    const savePhoto1 = async (profile, files, photo_dir) => {
        var player = null, PDB_ = null;
        var ext = files.file.originalFilename.match(/(\.svg|\.png|\.gif|\.jpg|\.jpeg)$/i)[0];
        try {
     log("got here for upload ...");
            // var photo = fs.readFileSync(files.file.filepath) ;
            PDB_ = await client.connect();
            log("uploading 1 ....");
            log("uploading 2 ....");
            player = await PDB_.query("SELECT P.player_id FROM chessdb.players P WHERE P.username = $1", [ profile.username ]); 
            const query = "INSERT INTO chessdb.profiles(profile_id, imagefile, filename, mimetype, filesize) VALUES($1, $2, $3, $4, $5) returning imagefile";
            const qres = await PDB_.query( query, [ player.rows[0].player_id, files.file.newFilename, files.file.originalFilename, files.file.mimetype, files.file.size ]);
            log("uploading 3 ....");
            log(qres);
            const newfile = photo_dir + "/" + files.file.newFilename,
                  renamedfile = photo_dir + "/" + player.rows[0].player_id + "_" + files.file.newFilename + ext;
            const ren = await fs.rename( newfile, renamedfile,(err) => {
                    if (err) { return err };
                    return null;
                 });
            PDB_.release();
            if (ren != null)  return { code: 1125, msg: 'Photo failed to upload!' }
            return { code: 0, msg: 'Photo uploaded!', image: player.rows[0].player_id + "_" + files.file.newFilename + ext}
        } catch(error) {
            log("Insert err ....");
            if (error.code == 23505) {
    log("duplicated ...");
               try {
                    const profile = await PDB_.query("SELECT P.profile_id, P.imagefile, P.mimetype FROM chessdb.profiles P WHERE P.profile_id = $1", [ player.rows[0].player_id ]); 
                    const query = "UPDATE chessdb.profiles set imagefile = $1,  filename = $2, mimetype = $3, filesize = $4 where profile_id = $5";
                    const qres = await PDB_.query(query,
                       [ files.file.newFilename, files.file.originalFilename, files.file.mimetype, files.file.size, profile.rows[0].profile_id ]);
                    if (PDB_ != null) PDB_.release();
                    // delete old file and rename new file.
                    const oldfile = photo_dir + "/" + fullName(profile.rows[0].profile_id, profile.rows[0].imagefile, profile.rows[0].mimetype);
                          newfile = photo_dir + "/" + files.file.newFilename,
                      renamedfile = photo_dir + "/" + profile.rows[0].profile_id + "_" + files.file.newFilename + ext;
                    log("unlinking: " + oldfile);
             
                    const unl = await fs.unlink( oldfile, (err) => {
                         if (err) { return err };
                         return null;
                    } )
                    if (unl != null)  return { code: 1124, msg: 'Photo failed to upload!' }
                    const ren = await fs.rename( newfile, renamedfile,(err) => {
                         if (err) { return err };
                         return null;
                    } )
                    if (ren != null)  return { code: 1124, msg: 'Photo failed to upload!' }
                    return { code: 0, msg: 'Photo uploaded!', image: profile.rows[0].profile_id + "_" + files.file.newFilename + ext }
               } catch(error) {
                    log("Update err ....");
                    log(error);
                    if (PDB_ != null) PDB_.release();
                    return { code: error.code, msg: 'Photo failed to upload!' }
               }
            } 
            return { code: error.code, msg: 'Photo failed to upload!' }
        }
    }

    function calculateRating(game, tourney, wgames_so_far = 0, bgames_so_far = 0, machine = false) {
        const sA = scoreP(game.rt, WHITE), sB = scoreP(game.rt, BLACK);
        var kA = 40, kB = 40;

        const w30 = (wgames_so_far >= 30) ? true : false;
        const b30 = (bgames_so_far >= 30) ? true : false;

        var   variant  = null;
        const wrating  = clone(tourney.wgame.rating);
        const brating  = clone(tourney.bgame.rating);
        if (machine) {
           variant  = compvariants[tourney.settings.variant - 1].item;
        } else {
           variant  = variants[tourney.settings.variant - 1].item;
        }
        const gametype = timer_types[tourney.gametype - 1];
        const wrating_ = wrating[variant][gametype];
        const brating_ = brating[variant][gametype];

        kA = scaleF(wrating_.elo, w30); kB = scaleF(brating_.elo, b30);

        const new_elo = eloRating(wrating_.elo, brating_.elo, kA, kB, sA, sB);
        const new_wglicko = glicko2Rating(wrating_.glicko, wrating_.glicko_d, wrating_.glicko_v, [brating_.glicko],[brating_.glicko_d], [sA] );
        const new_bglicko = glicko2Rating(brating_.glicko, brating_.glicko_d, brating_.glicko_v, [wrating_.glicko],[wrating_.glicko_d], [sB] );
        const new_rmoo = ourRating(wrating_.rmoo, brating_.rmoo, new_elo, new_wglicko, new_bglicko);

        // update new ratings
        wrating_.elo = new_elo.rA;
        brating_.elo = new_elo.rB;

        wrating_.glicko   = new_wglicko.r;
        wrating_.glicko_d = new_wglicko.d;
        wrating_.glicko_v = new_wglicko.v;

        brating_.glicko   = new_bglicko.r;
        brating_.glicko_d = new_bglicko.d;
        brating_.glicko_v = new_bglicko.v;

        wrating_.rmoo = new_rmoo.rA;
        brating_.rmoo = new_rmoo.rB;

        const wpoints = { elo_p: new_elo.pA, glicko_p: new_wglicko.p, rmoo_p: new_rmoo.pA };
        const bpoints = { elo_p: new_elo.pB, glicko_p: new_bglicko.p, rmoo_p: new_rmoo.pB };

        return { wrating: wrating_, brating: brating_, wpoints:  wpoints, bpoints: bpoints };
    }
    
    function updateRating(game, tourney, nrating) {
        const wsession = getSession(game.wp); // SESSION_[game.wp];
        const bsession = getSession(game.bp); // SESSION_[game.bp];

        const ses_wrating  = wsession.rating;
        const ses_brating  = bsession.rating;

        const wrating  = tourney.wgame.rating;
        const brating  = tourney.bgame.rating;
        const variant  = recvariants[tourney.settings.variant - 1];
        const gametype = timer_types[tourney.gametype - 1];
        const wrating_ = wrating[variant][gametype]; 
        const brating_ = brating[variant][gametype]; 

        const ses_wrating_ = ses_wrating[variant][gametype];
        const ses_brating_ = ses_brating[variant][gametype];

        // update new ratings
        ses_wrating_.elo = wrating_.elo = nrating.wrating.elo;
        ses_brating_.elo = brating_.elo = nrating.brating.elo

        ses_wrating_.glicko   = wrating_.glicko   = nrating.wrating.glicko;  
        ses_wrating_.glicko_d = wrating_.glicko_d = nrating.wrating.glicko_d;
        ses_wrating_.glicko_v = wrating_.glicko_v = nrating.wrating.glicko_v;

        ses_brating_.glicko   = brating_.glicko   = nrating.brating.glicko;
        ses_brating_.glicko_d = brating_.glicko_d = nrating.brating.glicko_d;
        ses_brating_.glicko_v = brating_.glicko_v = nrating.brating.gilcko_v;

        ses_wrating_.rmoo = wrating_.rmoo = nrating.wrating.rmoo;
        ses_brating_.rmoo = brating_.rmoo = nrating.brating.rmoo;

    }

    const getScore = async (game, tourney) => {
        var PDB_ = null, res = null;
        if (tourney.utype == GUEST) {
            const nrating = await calculateRating(game, tourney, 0, 0);
            return nrating;
        } else {
          try {
              PDB_ = await client.connect();
              const wquery = 'SELECT P.*,  count(G.white_id) as numgames FROM ( SELECT P.player_id, P.firstname, P.lastname, P.fide_title ' +
                          ' FROM chessdb.players P where P.username = $1 ) P LEFT OUTER JOIN chessdb.games G ON ' +
                          ' P.player_id = G.white_id and G.variant = $2 and G.game_type = $3 group by P.player_id, P.firstname, ' + 
                          ' P.lastname, P.fide_title, G.white_id';
              const bquery = 'SELECT P.*, count(G.white_id) as numgames FROM ( SELECT P.player_id, P.firstname, P.lastname, P.fide_title ' +
                          ' FROM chessdb.players P where P.username = $1 ) P LEFT OUTER JOIN chessdb.games G ON ' +
                          ' P.player_id = G.black_id and G.variant = $2 and G.game_type = $3 group by P.player_id, P.firstname, P.lastname, ' + 
                          ' P.fide_title, G.black_id';
              const wplayer = await PDB_.query(wquery, [game.wp, tourney.settings.variant, tourney.gametype] );
              const bplayer = await PDB_.query(bquery, [game.bp, tourney.settings.variant, tourney.gametype] );

              if (wplayer.rowCount == 0) throw { code: 1000, msg: "Retrieving player info failed!", errid: 1004 };
              if (bplayer.rowCount == 0) throw { code: 1000, msg: "Retrieving player info failed!", errid: 1004 };

              const nrating = await calculateRating(game, tourney, wplayer.rows[0].numgames, bplayer.rows[0].numgames);
              var wrating = nrating.wrating, brating = nrating.brating;

              PDB_.release();
              return { rating: nrating, wplayerid: wplayer.rows[0].player_id, bplayerid: bplayer.rows[0].player_id };

           } catch(error) {
              if (PDB_ != null) PDB_.release();
              return null;
           }
        }
        return null;
    }

    async function update_rating_records(client, nrating, playerid, tourney) {
        const insert_query = "INSERT INTO chessdb.ratings (player_id, variant, timer_type, elo, glicko, glicko_d, glicko_v, rmoo) " +
                                 "VALUES ($1, $2, $3, $4, $5, $6, $7, $8 )";
        const update_query = "UPDATE chessdb.ratings set elo = $1, glicko = $2, glicko_d = $3, glicko_v = $4, rmoo = $5 WHERE " +
                                  "player_id = $6 and variant = $7 and timer_type = $8";
        try {
              await client.query( insert_query,
                  [ playerid, tourney.settings.variant, tourney.gametype,
                    nrating.elo, nrating.glicko, nrating.glicko_d, nrating.glicko_v, nrating.rmoo ]);
        } catch(err) {
               if (err.code == 23505) {
                  await client.query( update_query,
                     [ nrating.elo, nrating.glicko, nrating.glicko_d, nrating.glicko_v, nrating.rmoo,
                       playerid, tourney.settings.variant, tourney.gametype ]);
               }               
        }
    }
    
    const recordGame = async (game, tourney, nrating) => {
        var  code = 0, msg = "success";
        var PDB_ = await client.connect();
log("recording ...");
log(game);

        var elapsed = tourney.elapsed.join(',');

        try {
            const rating = nrating.rating;
            var wrating = rating.wrating, brating = rating.brating;
            // one transaction
            await PDB_.query('BEGIN')
            await update_rating_records(client, wrating, nrating.wplayerid, tourney);
            await update_rating_records(client, brating, nrating.bplayerid, tourney);
            await client.query(
                   "INSERT INTO chessdb.games (game_id, variant, game_type, white_id, black_id, welo, wglicko, wglicko_d, wglicko_v, wrmoo, " +
                   "belo, bglicko, bglicko_d, bglicko_v, brmoo, round, event, evt_location, eco, result, result_reason, date_played, moves, " + 
                   " elapsed, tournament_id, fen ) " +
                   "VALUES (nextval('chessdb.game_seq'), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, " + 
                                                         "$13, $14, $15, $16, $17, $18, $19, $20, to_timestamp($21), $22, $23, $24, $25)",
                    [ tourney.settings.variant, tourney.gametype, nrating.wplayerid, nrating.bplayerid, 
                      wrating.elo, wrating.glicko, wrating.glicko_d, wrating.glicko_v, wrating.rmoo, 
                      brating.elo, brating.glicko, brating.glicko_d, brating.glicko_v, brating.rmoo, 
                      tourney.round, game.ev, game.si,  game.eco, game.rt,  game.reason, UTCtoday()/1000, game.moves, elapsed, 
		      tourney.tournament_id, game.fen ]);
            PDB_.query('COMMIT')
            updateRating(game, tourney, rating);

        } catch (error) {
          PDB_.query('ROLLBACK')
            code = error.code; msg: error.msg; 
        } finally {
            PDB_.release();
        }
        return { code: code, msg: msg }
    };

    function isSimulOver(simul) {
     var len = simul.winner.length;
     var over = false;
     if (simul.joined == len) { over = true; } 
     return over; 
    }

    async function recordSimul(socket, user, game) {
         const simul = getSimul(game.gametoken); //Simul[game.gametoken];
         const sgame = simul_players = getSimulPlayers(game.gametoken); // Simul_Players[game.gametoken];

         if (simul != null) {
               var tourney = {};
               tourney.gametype = gameType(simul.settings.gamemin);
               tourney.settings = simul.settings;
               tourney.utype    = simul.utype;
               tourney.moves    = simul.moves;
               tourney.elapsed  = simul.elapsed;

               var host   = (sgame.host.player == game.wp) ? game.wp : game.bp;
               var player = (sgame.host.player == game.wp) ? sgame.players[game.bp] : sgame.players[game.wp];
               var host_color = sgame.host.host_color;
        

               if (host_color == WHITE) {
                    tourney.bgame = player;
                    tourney.wgame = { player: sgame.host.player, gameid: player.host_gameid, socketid: sgame.host.socketid, color: host_color };
               } else {
                    tourney.wgame = player;
                    tourney.bgame = { player: sgame.host.player, gameid: player.host_gameid, socketid: sgame.host.socketid, color: host_color };
               }
               tourney.bgame.rating = getSession(tourney.bgame.player).rating; 
               tourney.wgame.rating = getSession(tourney.wgame.player).rating; // SESSION_[tourney.wgame.player].rating;
               tourney.bgame.ratesystem = getSession(tourney.bgame.player).ratesystem; // SESSION_[tourney.bgame.player].ratesystem;
               tourney.wgame.ratesystem = getSession(tourney.wgame.player).ratesystem; // SESSION_[tourney.wgame.player].ratesystem;
log("simul tourney ...");
log(tourney);
                  var res = null, other_game = null, our_game = null, winner_record = false, nrating = null, winner_color = null;
                  if (user.username == game.wp && ( game.rt == "1-0" || game.rt == "1/2-1/2")) { // let white winner record
                                nrating = await getScore(game, tourney);
                                if (simul.utype == REGISTERED) res = recordGame(game, tourney, nrating);
                                other_game = tourney.bgame;
                                our_game  = tourney.wgame;
                                winner_record = true;
                                simul.winner.push(user.username);
                  } else
                  if (user.username == game.bp && game.rt == "0-1") { // let black winner record
                                nrating = await getScore(game, tourney);
                                if (tourney.utype == REGISTERED) res = recordGame(game, tourney, nrating);
                                other_game = tourney.wgame;
                                our_game  = tourney.bgame;
                                winner_record = true;
                                tourney.bgame.player.win = true;
                                tourney.wgame.player.win = false;
                                simul.winner.push(user.username);
                  }
                  if (nrating != null) {
log("simul rating ...");
log(nrating);
                      if (winner_record) {
log("who is the winner?");
log(sgame);
                                simul.active   = false;
                                simul.gameover = isSimulOver(simul);

                                const other_res = { code: 0, msg: 'success' };
                                const psocketid       = other_game.socketid;
                                other_res.username    = getSession(other_game.player).username;  // SESSION_[ other_game.player ].username;
                                other_res.sessionid   = getSession(other_game.player).sessionid; // SESSION_[ other_game.player ].sessionid;
                                other_res.gameid      = other_game.gameid;
                                other_res.ratesystem  = other_game.ratesystem;
                                other_res.rating      = (tourney.utype == GUEST) ? nrating : nrating.rating;
                                other_res.status      = game.status; // status from individual games 

                                const our_res = { code: 0, msg: 'success' };
                                our_res.username    = getSession(our_game.player).username; // SESSION_[ our_game.player ].username;
                                our_res.sessionid   = getSession(our_game.player).sessionid; // SESSION_[ our_game.player ].sessionid;
                                our_res.gameid      = our_game.gameid;
                                our_res.ratesystem  = our_game.ratesystem;
                                our_res.rating      = (tourney.utype == GUEST) ? nrating : nrating.rating;
                                our_res.status      = game.status; // status from individual games
                        
                                other_res.rt        = game.rt;
                                our_res.rt          = game.rt;


                                if (tourney.utype == REGISTERED) {
                                    res.then((res) => {
                                      if (res.code == 0) {
                                          socket.broadcast.to(psocketid).emit('recordedgame', other_res ); // for the others.
                                          socket.emit("recordedgame", our_res);
                                      } else {
                                          socket.emit('GameError', 'Unable to sync game in server ...');
                                      }
                                    });
                                 } else { // for guests
                                    socket.broadcast.to(psocketid).emit('recordedgame', other_res ); // for the others.
                                    socket.emit("recordedgame", our_res);
                                 }
                      }
                  } else {
                      socket.emit('GameError', 'Unable to calculate score  ...');
                  } 
         } else {
               socket.emit('GameError', 'Unable to find the game  ...');
         }
    }

   async function recordMatch(socket, user, game) {
         const challenge = getMatch(game.gametoken); // Tourneys[game.gametoken];

         if (challenge != null && challenge.wgame.player == game.wp && challenge.bgame.player == game.bp) {

                     var res = null, other_game = null, our_game = null, winner_record = false, nrating = null;
log("what ??? ");
log(game);
log(user.username);
log(game.wp);
log(game.bp);
log(game.rt);
                     if (user.username == game.wp && ( game.rt == "1-0" || game.rt == "1/2-1/2")) { // let white winner record
log("white ...");
                                nrating = await getScore(game, challenge);
                                if (challenge.utype == REGISTERED) res = recordGame(game, challenge, nrating);
                                other_game = challenge.bgame;
                                our_game  = challenge.wgame;
                                winner_record = true;
                     } else
                     if (user.username == game.bp && game.rt == "0-1") { // let black winner record
log("black ...");
                                nrating = await getScore(game, challenge);
                                if (challenge.utype == REGISTERED) res = recordGame(game, challenge, nrating);
                                other_game = challenge.wgame;
                                our_game  = challenge.bgame;
                                winner_record = true;
                     }
log("rating ...");
log(nrating);
                     if (nrating != null) {
log("challenge rating ...");
log(nrating);
                         if (winner_record) {
                                const other_res = { code: 0, msg: 'success' };
                                const psocketid       = other_game.socketid;
                                other_res.username    = getSession(other_game.player).username; // SESSION_[ other_game.player ].username;
                                other_res.sessionid   = getSession(other_game.player).sessionid; // SESSION_[ other_game.player ].sessionid;
                                other_res.gameid      = other_game.gameid;
                                other_res.ratesystem  = other_game.ratesystem;
                                other_res.rating     = (challenge.utype == GUEST) ? nrating : nrating.rating;
                                other_res.status      = game.status;

                                const our_res = { code: 0, msg: 'success' };
                                our_res.username    = getSession(our_game.player).username; // SESSION_[ our_game.player ].username;
                                our_res.sessionid   = getSession(our_game.player).sessionid; // SESSION_[ our_game.player ].sessionid;
                                our_res.gameid      = our_game.gameid;
                                our_res.ratesystem  = our_game.ratesystem;
                                our_res.rating     = (challenge.utype == GUEST) ? nrating : nrating.rating;
                                our_res.status      = game.status;

                                challenge.active = false;

                                other_res.rt        = game.rt;
                                our_res.rt          = game.rt;

                                if (challenge.utype == REGISTERED) {
                                    res.then((res) => {
                                      if (res.code == 0) {
                                          socket.broadcast.to(psocketid).emit('recordedgame', other_res ); // for the others.
                                          socket.emit("recordedgame", our_res);
                                      } else {
                                          socket.emit('GameError', 'Unable to sync game in server ...');
                                      }
                                    });
                                 } else { // for guests
                                    socket.broadcast.to(psocketid).emit('recordedgame', other_res ); // for the others.
                                    socket.emit("recordedgame", our_res);
                                 }
                         }

                     } else {
                         socket.emit('GameError', 'Unable to calculate score  ...');
                     }
         } else {
               socket.emit('GameError', 'Unable to find the game  ...');
         }
    }

    async function recordTourney(socket, user, game) {
         const tourney = getTourney(game.gametoken); // Tourneys[game.gametoken];

         if (tourney != null) {
 
              const games = tourney.pairing.plan[tourney.currentround];
              const seat  = tourney.players;
              var wgame = null, bgame = null, wsession = null, bsession = null, elapsed = null;

              for (var g in games) {
                       var game_ = games[g];
                       var white = seat[game_.white];
                       var black = seat[game_.black];
                       if (white.player.username == game.wp && black.player.username == game.bp) {
                          wsession = getSession(white.player.username);
                          bsession = getSession(black.player.username);
                          if (wsession != null)
                          wgame = { player: wsession.username, gameid: game_.wgameid, rating: wsession.rating, 
				    title: wsession.title, ratesystem: wsession.ratesystem, socketid: wsession.socketid };
                          if (bsession != null)
                          bgame = { player: bsession.username, gameid: game_.bgameid, rating: bsession.rating, 
                                    title: bsession.title, ratesystem: bsession.ratesystem, socketid: bsession.socketid };
                          elapsed = game_.elapsed;

                          // record score of players
                          if (user.username == game.wp) {
                               if (game.rt == '1-0') {
                                   white.score++;
                                   white.result.push(1);
                               } else
                               if (game.rt == '0-1') {
                                   white.result.push(0);
                               } else
                               if (game.rt == '1/2-1/2') {
                                   white.score += 0.5;
                                   white.result.push(0.5);
                               }
                          } else 
                          if (user.username == game.bp) {
                               if (game.rt == '0-1') {
                                   black.score++;
                                   black.result.push(1);
                               } else
                               if (game.rt == '1-0') {
                                   black.result.push(0);
                               } else
                               if (game.rt == '1/2-1/2') {
                                   black.score += 0.5;
                                   black.result.push(0.5);
                               }
                          } 
                          game_.gameover = true; // this is initialized from notifyToStartGames() function
                          break;
                       }
              }
log("get wgame and bgame ...");
log(tourney.players);

              if (wgame != null && bgame != null) {
log("got in ...");

                     const tourney_ = { settings: { variant: tourney.variant + 1, timer: tourney.timer, inc: tourney.increment },
                                        gametype: gameType(tourney.timer), gametoken: game.gametoken, simul: false,
                                        round: tourney.currentround, wgame: wgame, bgame: bgame, elapsed: elapsed,
					tournament_id: game.gametoken, fen: game.fen  };

                     var res = null, other_game = null, our_game = null, winner_record = false, nrating = null;

                     if (user.username == game.wp && ( game.rt == "1-0" || game.rt == "1/2-1/2")) { // let white winner record
log("white ...");
                                nrating = await getScore(game, tourney_);
                                if (wsession.utype == REGISTERED) res = recordGame(game, tourney_, nrating);
                                other_game = tourney_.bgame;
                                our_game   = tourney_.wgame;
                                winner_record = true;
                     } else
                     if (user.username == game.bp && game.rt == "0-1") { // let black winner record
log("black ...");
log(game);
log(tourney_);
                                nrating = await getScore(game, tourney_);
                                if (bsession.utype == REGISTERED) res = recordGame(game, tourney_, nrating);
                                other_game = tourney_.wgame;
                                our_game   = tourney_.bgame;
                                winner_record = true;
                     }
log("rating ...");
log(nrating);
                     if (nrating != null) {
log("tourney rating ...");
log(nrating);
                         if (winner_record) {
                                const other_res = { code: 0, msg: 'success' };
                                const psocketid       = other_game.socketid;
                                other_res.username    = getSession(other_game.player).username; // SESSION_[ other_game.player ].username;
                                other_res.sessionid   = getSession(other_game.player).sessionid; // SESSION_[ other_game.player ].sessionid;
                                other_res.gameid      = other_game.gameid;
                                other_res.ratesystem  = other_game.ratesystem;
                                other_res.rating      = nrating.rating;
                                other_res.status      = game.status;

                                const our_res = { code: 0, msg: 'success' };
                                our_res.username    = getSession(our_game.player).username; // SESSION_[ our_game.player ].username;
                                our_res.sessionid   = getSession(our_game.player).sessionid; // SESSION_[ our_game.player ].sessionid;
                                our_res.gameid      = our_game.gameid;
                                our_res.ratesystem  = our_game.ratesystem;
                                our_res.rating      = nrating.rating;
                                our_res.status      = game.status;

                                other_res.rt        = game.rt;
                                our_res.rt          = game.rt;

                                res.then((res) => {
                                      if (res.code == 0) {
                                          socket.broadcast.to(psocketid).emit('recordedgame', other_res ); // for the others.
                                          socket.emit("recordedgame", our_res);
                                      } else {
                                          socket.emit('GameError', 'Unable to sync game in server ...');
                                      }
                                });

                                /* update distributed system */
                                setTourney(tourney.tournament_id, tourney);

                                /* update pairing */
                                updatePairing(tourney);
                         }

                     } else {
                         socket.emit('GameError', 'Unable to calculate score  ...');
                     }
              } else {
                  socket.emit('GameError', 'Unable to find the matching game  ...');
              }
         } else {
               socket.emit('GameError', 'Unable to find the tournament  ...');
         }
    } 
    
    
    function voidGameInQueue(gametoken) {
      for (var p in GameQueue) {
        var inqueue = GameQueue[p];
        for (var x in gametoken) {
          var pass = gametoken[x];
          if (inqueue.gametoken == pass) inqueue.void = true;
        }
      }
    }
    
    function dislodgeFromSession(psession) {
        for (var p in SESSION_) {
          const session = getSession(p); // SESSION_[p];
log("dislodging from this session ...");
          if (session.sessionid == psession.sessionid) {
             unsetSession(p); // delete SESSION_[p];
            
             log("User: " + session.username + " dislodged ...");
             var player = PlayerInQueue[p];
             if (player != null) {
                 var pass = [];
                 for (var p in player) {
                    var inqueue = player[p];
                    var tourney = getMatch(inqueue.gametoken); // Tourneys[inqueue.gametoken];
                    if (tourney != null) {
                      // Tourneys[inqueue.gametoken].abandonedby = session.username;
                      tourney.abandonedby = session.username;
                    }
                    pass.push(inqueue.gametoken);
                    delete player[p];
                 }
                 delete PlayerInQueue[p];
                 voidGameInQueue(pass);
             }
    
             break;
          }
        }
    }
    
    function reQueue(queue) {
       while(queue.length) {
           const offer = queue.pop();
           GameQueue.unshift(offer);
       }
    }

    function isCompatible(match, qmatch_) {
       var mgametype = gameType(match.config.gamemin);
       const msess_ = getSession(match.username); // SESSION_[match.username]; // player that we are.
       const qsess_ = getSession(qmatch_.username); // SESSION_[qmatch_.username];   // player we're seeking for match
       const mrating = msess_.rating;
       const qrating = qsess_.rating;
       var   rating  = false;
       var rnd_color = (match.config.piece == qmatch_.settings.piece && match.config.piece == 3);  // random color
       var color = rnd_color ||
                        (match.config.piece == 1 && qmatch_.settings.piece == 2) || 
                        (match.config.piece == 2 && qmatch_.settings.piece == 1);
log("the color ...");
log(color);
       var israted = ((match.config.israted && qmatch_.settings.israted) || (!match.config.israted && !qmatch_.settings.israted));
       var istitled = ((match.config.istitled && qmatch_.settings.istitled) || (!match.config.istitled && !qmatch_.settings.istitled));
log("israted and istitled");
log(israted);
log(istitled);
       if (israted && istitled && color && match.utype == qmatch_.utype) {
            var mvariant = match.config.variant;
            var qvariant = qmatch_.settings.variant;
log("got here for types ...");

            if (mvariant == qvariant) {  // is it standard, chess960, crazyhouse, etc...?
                   var qgametype = qmatch_.gametype;
                   if (mgametype == qgametype) {  // is it bullet, blitz, rapid, or classical?
                      if (match.config.gamemin == qmatch_.settings.gamemin) {
                         if (match.config.gameinc == qmatch_.settings.gameinc) {
                            // now let's compare rating systems.
                            const range   = possible_ranges[match.config.gamerange];
                            const qrange_ = possible_ranges[qmatch_.settings.gamerange];
                            if ( (qrange_.mi <= range.mi && range.mi <= qrange_.ma) ||
                                 (range.mi <= qrange_.mi && qrange_.mi <= range.ma) ) {
                                  var mvariant_ = recvariants[mvariant - 1];
                                  var qvariant_ = recvariants[qvariant - 1];
                                  var mtype     = timer_types[mgametype - 1];
                                  var qtype     = timer_types[mgametype - 1];
                                  var mrating_  = mrating[mvariant_][mtype];
                                  var qrating_  = qrating[qvariant_][qtype];
                                  var msystem   = msess_.ratesystem;
                                  var qsystem   = qsess_.ratesystem;
                                  var mrate = false, qrate = false;
                                  if (msystem == 'elo')     mrate = (range.mi <= qrating_.elo <= range.ma); else
                                  if (msystem == 'rmoo')    mrate = (range.mi <= qrating_.rmoo <= range.ma); else
                                  if (msystem == 'glicko')  mrate = (range.mi <= qrating_.glicko <= range.ma);
                                  if (qsystem == 'elo')     qrate = (qrange_.mi <= mrating_.elo <= qrange_.ma); else
                                  if (qsystem == 'rmoo')    qrate = (qrange_.mi <= mrating_.rmoo <= qrange_.ma); else
                                  if (qsystem == 'glicko')  qrate = (qrange_.mi <= mrating_.glicko <= qrange_.ma);
                                  return (mrate && qrate);
                                  
                            }
                         }
                      }
                   }
            }
       }
       return false;
    }

    function gameColor(piece) {
       if (piece == 1) return WHITE;
       if (piece == 2) return BLACK;
       if (piece == 3) { const rnd = Math.random(); return (rnd >= 0.5) ? BLACK : WHITE; }
    }
    
    function gameType(gamemin) {
       var timer = 0, gametype = 4;
       if (gamemin == 0) timer = 0.50; else
       if (gamemin == 1) timer = 1.00; else
       if (gamemin == 2) timer = 1.50; else
       if (gamemin == 3) timer = 2.00; else
       if (gamemin == 4) timer = 2.50; else timer = parseInt(possible_timer[gamemin]);
       if (timer < 3)   gametype = 1; else  /* bullet */
       if (timer <= 10) gametype = 2; else  /* blitz */
       if (timer < 59)  gametype = 3;       /* rapid */
       return gametype;
    }
    
    function findPlayer(match) {
       var player = null, pass = null;
       var tempqueue = [];
    log("find player ...");
    
       while (GameQueue.length) {
            const qmatch_ = pl_ = GameQueue.pop();
    log("player vs match");
    log(qmatch_);
    log(match)
            if (qmatch_.void) continue;
            if (qmatch_.username == match.username) {
                // ignore our own offer
                 if (match.gameid == qmatch_.gameid) { 
                       GameQueue.unshift(qmatch_);
                       while(tempqueue.length) {
                           const offer = tempqueue.pop();
                           GameQueue.unshift(offer);
                       }
    log("so we found user and board matching ... shall we rely on queueing??");
    	           return null; // already in queue. let requeue and wait for acceptance
                 } else {  // see if there are other offers.
                       if (!GameQueue.length) { // no other offers.
                           GameQueue.unshift(qmatch_);
    log("so we found user but no board matching ... shall we rely on queueing ??");
                           return null;
                       } else { // we have other offers, and we cannot just take our own offer.
    log("so we found user but no board matching ... shall we rely on queueing 2??");
                         tempqueue.push(qmatch_);
                         continue;
                       }
                 }
            } 
    
            if (isCompatible(match, qmatch_)) {
                reQueue(tempqueue);
                return qmatch_;
            }
            tempqueue.push(qmatch_); 
       }
       reQueue(tempqueue);
       log("returning with nothing ...");
       return null;
    }
    
    function dislodgeFromQueue(player) {
    log("dispose original queue");
    log(player);
    log(PlayerInQueue);
         const offer = PlayerInQueue[ player.username ];
         const temp_ = [];
         if (offer != null) {
              if (player.gametoken != null)
              while(offer.length) {
                  const inqueue = offer.pop();
                  if (inqueue != null && typeof(inqueue.gametoken) != "undefined") {
                    if (player.gameid == inqueue.gameid) {
                      /* do nothing */
                    } else {
                      temp_.push(inqueue);
                    }
                  }
              }
              while(temp_.length) { 
                    var inqueue = temp_.pop();
                    if (typeof(inqueue) != "undefined") {
       		    PlayerInQueue[ player.username ].push(inqueue);
                    }
              }
              if (!offer.length) delete PlayerInQueue[ player.username ];
    
              // this one is expensive, but let's do with it for now
              for (var p in GameQueue) {
                const inqueue = GameQueue[p];
                if (player.username == inqueue.username && player.gameid == inqueue.gameid) {
                   inqueue.void = true;
                }
              }
        }
    }

    function chooseBoard(boards) {
      for (var p in boards) {
         var board = boards[p];
         if (!board.istaken) return board;
      }
      return null;
    }

    async function joinSimul(socket, session, profile) {
        const simul = getSimul(profile.gametoken); // Simul[profile.gametoken];
        const simul_players = getSimulPlayers(profile.gametoken); // Simul_Players[profile.gametoken];
        if (simul != null) {
          if (simul.utype != profile.utype && profile.utype == GUEST) {
log("update 1 ...");
               // guests cannot join registered games. registered players can join guests' hosted games.
               socket.emit('joinedsimul', { declined: true } );
          } else
          if (simul_players.players[session.username] == null)  { // player not in game yet
log("update 2 ...");
             var host_board = chooseBoard(simul_players.host.boards);
log(simul_players.host.boards);
log(host_board);
             if (simul.joined < simul.total && host_board != null) {
log("update 3 ...");
               var user_game = null, mygameid = null, rating = null;
               var color = (simul.host_color == WHITE) ? BLACK : WHITE;
               simul_players.players[session.username] = { player: session.username, gameid: profile.gameid, 
                                                           host_gameid: host_board.gameid, socketid: socket.id, color: color, result: null };
               simul.joined = simul.joined + 1;
               simul.complete = parseInt(simul.joined) == parseInt(simul.total);
               if (simul.complete) simul.started = true;

               // let's build structure for client to use for subsequent rematches (e.g. board.rematch_)
               const game = clone(simul);
               delete game.moves;
               delete game.elapsed;
               if (session.rating != null) rating = session.rating;
               if (simul.host_color == WHITE) { // host
                  game.wgame = { player: simul.host, gameid: host_board.gameid, rating: simul.host_rating, color: WHITE }
                  game.bgame = { player: session.username, gameid: profile.gameid, rating: rating, color: BLACK };
                  host_board.istaken = true;
               } else {
                  game.wgame = { player: session.username, gameid: profile.gameid, rating: rating, color: WHITE };
                  game.bgame = { player: simul.host, gameid: host_board.gameid , rating: simul.host_rating, color: BLACK};
                  host_board.istaken = true;
               }

               var boards = [];
               if (simul.complete) { // build a board list for room broadcast to start game
                    for (var p in simul_players.players) {
                      var who_ = simul_players.players[p];
                      boards.push({ player: who_.player, gameid: who_.gameid, host: simul.host, host_gameid: who_.host_gameid }); 
                    }                  
                    user_game = { newlyjoined: session.username, game: game, boards: boards };
               } else {
                    user_game = { newlyjoined: session.username, game: game, boards: null };
               }

               socket.join(game.room); 
               io.sockets.in(game.room).emit('joinedsimul', user_game );

               // if other player originally requested for a new match that is queued prior to rematch
               // then let us clear the offer from the queue.
               for (var p in profile.othergames) {
                   var gameid = profile.othergames[p];
                   dislodgeFromQueue({ username: profile.username, gameid: profile.gameid } );
               }

             } else {
               socket.emit('joinedsimul', { complete: true } );
             }
          }
          log("Simul ...");
          log(simul);
          log("Simul Players ...");
          log(simul_players);
        }
    }

    async function resign(socket, session, profile) {
        var game = profile.game;
        if (game.simul) {
            const simul = getSimul(game.gametoken); // Simul[game.gametoken];
            const simul_players = getSimulPlayers(game.gametoken); // Simul_Players[game.gametoken];
log("game ...");
log(game);
log("simul ...");
log(simul);
log("simul players ...");
log(simul_players);
            const wgame = game.wgame;
            const bgame = game.bgame;
            const host = simul_players.host;
            if (simul.host == profile.username) {  // the host is resigning from a game.
                for (var p in simul_players.players) {
                    var who_ = simul_players.players[p];
                    socket.broadcast.to(who_.socketid).emit("resign", { username: who_.player, requestor: profile.username, game: game } );
                }
            } else { // else one of the participants is resigning
                for (var p in simul_players.players) {
                    var who_ = simul_players.players[p];
                    if (who_.player == profile.username) {  // this player is resigning
                       socket.broadcast.to(host.socketid).emit("resign", { username: host.player, requestor: profile.username, game: game } );
                       break;
                    }
                }
            }
        } else {
log("resigning");
            const tourney = getMatch(game.gametoken); // Tourneys[game.gametoken];
            var wgame = tourney.wgame;
            var bgame = tourney.bgame;
            if (wgame.player == profile.username) {
log("sending response for resignation 1");
                socket.broadcast.to(bgame.socketid).emit("resign", { username: bgame.player, requestor: profile.username, game: game } );
            } else {
log("sending response for resignation 2");
                socket.broadcast.to(wgame.socketid).emit("resign", { username: wgame.player, requestor: profile.username, game: game } );
            }
        }
    }

    async function takeback(socket, session, profile) {
        var game = profile.game;
        if (game.simul) {
            const simul = getSimul(game.gametoken); // Simul[game.gametoken];
            const simul_players = getSimulPlayers(game.gametoken); // Simul_Players[game.gametoken];
            /* we do not support takebacks in simul games */
        } else {
            const tourney = getMatch(game.gametoken); // Tourneys[game.gametoken];
log("request for takeback ...");
log(profile);
log(tourney);
            var wgame = tourney.wgame;
            var bgame = tourney.bgame;
            if (wgame.player == profile.username) {
log("got here 1 ...");
log(bgame.socketid);
log(bgame.player);
log(profile.username);
                socket.broadcast.to(bgame.socketid).emit("takeback", { username: bgame.player, requestor: profile.username, game: game } );
            } else {
log("got here 2 ...");
log(wgame.socketid);
log(wgame.player);
log(profile.username);
                socket.broadcast.to(wgame.socketid).emit("takeback", { username: wgame.player, requestor: profile.username, game: game } );
            }
        }
    }

    async function acceptTakeback(socket, session, profile) {
        var game = profile.game;
        if (game.simul) {
            const simul = getSimul(game.gametoken); // Simul[game.gametoken];
            const simul_players = getSimulPlayers(game.gametoken); // Simul_Players[game.gametoken];
            /* we do not support takebacks in simul games */
        } else {
            const tourney = getMatch(game.gametoken); // Tourneys[game.gametoken];
log("accepting takeback ...");
log(profile);
log(tourney);
            var wgame = tourney.wgame;
            var bgame = tourney.bgame;
            if (wgame.player == profile.username) {
log("got here 1 ...");
log(bgame.socketid);
log(bgame.player);
log(profile.username);
                socket.broadcast.to(bgame.socketid).emit("accepttakeback", { username: bgame.player, requestor: profile.username, game: game } );
            } else {
log("got here 2 ...");
log(wgame.socketid);
log(wgame.player);
log(profile.username);
                socket.broadcast.to(wgame.socketid).emit("accepttakeback", { username: wgame.player, requestor: profile.username, game: game } );
            }
        }
    }

    async function abortGame(socket, session, profile) {
        var game = profile.game;
        if (game.simul) {
            const simul = getSimul(game.gametoken); // Simul[game.gametoken];
            const simul_players = getSimulPlayers(game.gametoken); // Simul_Players[game.gametoken];
            const wgame = game.wgame;
            const bgame = game.bgame;
            const host = simul_players.host;
            if (simul.host == profile.username) {  // the host is resigning from a game.
                for (var p in simul_players.players) {
                    var who_ = simul_players.players[p];
                    socket.broadcast.to(who_.socketid).emit("abortgame", { username: who_.player, requestor: profile.username, game: game } );
                }
            } else { // else one of the participants is resigning
                for (var p in simul_players.players) {
                    var who_ = simul_players.players[p];
                    if (who_.player == profile.username) {  // this player is resigning
                       socket.broadcast.to(host.socketid).emit("abortgame", { username: host.player, requestor: profile.username, game: game } );
                       break;
                    }
                }
            }
        } else {
            const tourney = getMatch(game.gametoken); // Tourneys[game.gametoken];
log("offering abort game ...");
log(profile);
log(tourney);
            var wgame = tourney.wgame;
            var bgame = tourney.bgame;
            if (wgame.player == profile.username) {
log("got here 1 ...");
log(bgame.socketid);
log(bgame.player);
log(profile.username);
                socket.broadcast.to(bgame.socketid).emit("abortgame", { username: bgame.player, requestor: profile.username, game: game } );
            } else {
log("got here 2 ...");
log(wgame.socketid);
log(wgame.player);
log(profile.username);
                socket.broadcast.to(wgame.socketid).emit("abortgame", { username: wgame.player, requestor: profile.username, game: game } );
            }
        }
    }

    async function acceptAbort(socket, session, profile) {
        var game = profile.game;
        if (game.simul) {
            const simul = getSimul(game.gametoken); // Simul[game.gametoken];
            const simul_players = getSimulPlayers(game.gametoken); // Simul_Players[game.gametoken];
            const wgame = game.wgame;
            const bgame = game.bgame;
            const host = simul_players.host;
            if (simul.host == profile.username) {  // the host is resigning from a game.
                for (var p in simul_players.players) {
                    var who_ = simul_players.players[p];
                    socket.broadcast.to(who_.socketid).emit("acceptabort", { username: who_.player, requestor: profile.username, game: game } );
                }
            } else { // else one of the participants is resigning
                for (var p in simul_players.players) {
                    var who_ = simul_players.players[p];
                    if (who_.player == profile.username) {  // this player is resigning
                       socket.broadcast.to(host.socketid).emit("acceptabort", { username: host.player, requestor: profile.username, game: game } );
                       break;
                    }
                }
            }
        } else {
            const tourney = getMatch(game.gametoken); // Tourneys[game.gametoken];
log("accepting abort game ...");
log(profile);
log(tourney);
            var wgame = tourney.wgame;
            var bgame = tourney.bgame;
            if (wgame.player == profile.username) {
log("got here 1 ...");
log(bgame.socketid);
log(bgame.player);
log(profile.username);
                socket.broadcast.to(bgame.socketid).emit("acceptabort", { username: bgame.player, requestor: profile.username, game: game } );
            } else {
log("got here 2 ...");
log(wgame.socketid);
log(wgame.player);
log(profile.username);
                socket.broadcast.to(wgame.socketid).emit("acceptabort", { username: wgame.player, requestor: profile.username, game: game } );
            }
        }
    }

    async function offerDraw(socket, session, profile) {
        var game = profile.game;
        if (game.simul) {
            const simul = getSimul(game.gametoken); // Simul[game.gametoken];
            const simul_players = getSimulPlayers(game.gametoken); // Simul_Players[game.gametoken];
            const wgame = game.wgame;
            const bgame = game.bgame;
            const host = simul_players.host;
            if (simul.host == profile.username) {  // the host is resigning from a game.
                for (var p in simul_players.players) {
                    var who_ = simul_players.players[p];
                    socket.broadcast.to(who_.socketid).emit("offerdraw", { username: who_.player, requestor: profile.username, game: game } );
                }
            } else { // else one of the participants is resigning
                for (var p in simul_players.players) {
                    var who_ = simul_players.players[p];
                    if (who_.player == profile.username) {  // this player is resigning
                       socket.broadcast.to(host.socketid).emit("offerdraw", { username: host.player, requestor: profile.username, game: game } );
                       break;
                    }
                }
            }
        } else {
            const tourney = getMatch(game.gametoken); // Tourneys[game.gametoken];
log("offering draw ...");
log(profile);
log(tourney);
            var wgame = tourney.wgame;
            var bgame = tourney.bgame;
            if (wgame.player == profile.username) {
log("got here 1 ...");
log(bgame.socketid);
log(bgame.player);
log(profile.username);
                socket.broadcast.to(bgame.socketid).emit("offerdraw", { username: bgame.player, requestor: profile.username, game: game } );
            } else {
log("got here 2 ...");
log(wgame.socketid);
log(wgame.player);
log(profile.username);
                socket.broadcast.to(wgame.socketid).emit("offerdraw", { username: wgame.player, requestor: profile.username, game: game } );
            }
        }
    }

    async function acceptDraw(socket, session, profile) {
        var game = profile.game;
        if (game.simul) {
            const simul = getSimul(game.gametoken); // Simul[game.gametoken];
            const simul_players = getSimulPlayers(game.gametoken); // Simul_Players[game.gametoken];
            const wgame = game.wgame;
            const bgame = game.bgame;
            const host = simul_players.host;
            if (simul.host == profile.username) {  // the host is resigning from a game.
                for (var p in simul_players.players) {
                    var who_ = simul_players.players[p];
                    socket.broadcast.to(who_.socketid).emit("acceptdraw", { username: who_.player, requestor: profile.username, game: game } );
                }
            } else { // else one of the participants is resigning
                for (var p in simul_players.players) {
                    var who_ = simul_players.players[p];
                    if (who_.player == profile.username) {  // this player is resigning
                       socket.broadcast.to(host.socketid).emit("acceptdraw", { username: host.player, requestor: profile.username, game: game } );
                       break;
                    }
                }
            }
        } else {
            const tourney = getMatch(game.gametoken); // Tourneys[game.gametoken];
log("accepting draw ...");
log(profile);
log(tourney);
            var wgame = tourney.wgame;
            var bgame = tourney.bgame;
            if (wgame.player == profile.username) {
log("got here 1 ...");
log(bgame.socketid);
log(bgame.player);
log(profile.username);
                socket.broadcast.to(bgame.socketid).emit("acceptdraw", { username: bgame.player, requestor: profile.username, game: game } );
            } else {
log("got here 2 ...");
log(wgame.socketid);
log(wgame.player);
log(profile.username);
                socket.broadcast.to(wgame.socketid).emit("acceptdraw", { username: wgame.player, requestor: profile.username, game: game } );
            }
        }
    }

    async function cancelSimul(socket, session, profile) {
        const game = getSimul(profile.gametoken); // Simul[profile.gametoken];
        const simul_players = getSimulPlayers(profile.gametoken); // Simul_Players[profile.gametoken];
log("cancelSimul ...");
log(game);
        if (game) {
log("canceling ...");
           const room = game.room;
           unsetSimul(profile.gametoken);
           unsetSimulPlayers(profile.gametoken);
           unsetSimulHost(profile.username);

           // delete Simul[profile.gametoken];
           // delete Simul_Players[profile.gametoken];
           // delete Simul_Hosts[profile.username];
           socket.broadcast.to(room).emit("canceledsimul", { status: 'cancelled' });
           socket.emit("canceledsimul", { status: 'cancelled' });
log(Simul);
        }
    }

    function alreadyHosting(session) {
        const host =  getSimulHost(session.username); // Simul_Hosts[session.username];
        if (host != null) {
            const simul =  getSimul(host.token); // Simul[host.token];
            if (simul != null) {
               if (simul.complete) return false;
            }
            return (host != null);
        }
        return false;
    }

/* Remove - obsolete
    async function gameboardTournaments(socket, session, boards) {
        const simul_players = Simul_Players[boards.gametoken];
        for (var userid in boards.games) {
          var gameid = boards.games[userid];
          var user = simul_players.players[userid];
          user.host_gameid = gameid;
        }
    }
*/

    function genMOLE(variant) {
        if (variant == THEMOLE || variant == THETRAITOR) {
           var mole = Math.round(Math.random() * 14)  /* 0-7=P, 8-11=N, 13-14=R */
           var wmole = null, bmole = null, idx = null;
           if (mole >= 0 && mole <=9) {
              wmoles = ['a2','b2','c2','d2','e2','f2','g2','h2'];
              bmoles = ['a7','b7','c7','d7','e7','f7','g7','h7'];
              idx = Math.round(Math.random() * 7); wmole = wmoles[idx];
              idx = Math.round(Math.random() * 7); bmole = bmoles[idx];
         
           } else
           if (mole >= 10 && mole <= 12) {
              wmoles = ['b1','g1'];
              bmoles = ['b8','g8'];
              idx = Math.round(Math.random() * 1); wmole = wmoles[idx];
              idx = Math.round(Math.random() * 1); bmole = bmoles[idx];
           } else
           if (mole >= 13 || mole <= 14) {
              wmoles = ['a1','h1'];
              bmoles = ['a8','h8'];
              idx = Math.round(Math.random() * 1); wmole = wmoles[idx];
              idx = Math.round(Math.random() * 1); bmole = bmoles[idx];
           }
           return { wmole: wmole, bmole: bmole }
        }
        return null;
    }
    
    async function simulTournaments(socket, session, match) {
        var now = Date.now(), found = false;
        var room = 'simul_' + session.username;
        var simultoken_ = await bcrypt.hash('rmoo' + now, Math.random());
        var color = gameColor(match.config.piece);
        var gametype = gameType(match.config.gamemin);
        var variant = variants[match.config.variant - 1].item;
        var fen = wINITFEN_;
        var start_date = UTCtoday() + 10 * 60 * 1000;
        if (variant == CHESS960) fen = randomFENchess960();
        if (variant == HORDE) { fen = wINITFENHORDE_ };

        var MOLE = genMOLE(variant);
        const simul_ = { host: session.username, room: room, gametoken: simultoken_, gametype: gametype, simul: true, active: false,  fen: fen, mole: MOLE,
                         joined: 0, total: match.total, settings: match.config, utype: match.utype, moves: [], elapsed: [], winner: [], gameover: false,
                         complete: false, host_color: color, host_rating: session.rating, registered: UTCtoday(), start_date: start_date, started: false };
log("simultaneous 1 ...");
log(simul_);
        if (!alreadyHosting(session)) {
log("simultaneous 2 ...");
           // Simul[simultoken_] = simul_;
           setSimul(simultoken_, simul_); 
           const splayers = { players: {}, host: { player: session.username, boards: match.boards, socketid: socket.id, host_color: color }  };
           //Simul_Players[simultoken_] = splayers;
           setSimulPlayers(simultoken_, splayers); 
           // Simul_Hosts[session.username] = { token: simultoken_ };
           setSimulHost(session.username, { token: simultoken_ } );
           socket.join(room);
           socket.emit("readytohost", { token: simultoken_, settings: match.config, host: session.username, host_color: color } );
        } else {
log("simultaneous 3 ...");
           socket.emit("alreadyhostingsimul", { token: simultoken_ } );
        }
    }


    async function matchGame(socket, session, match) {
          var player = findPlayer(match);
          if (player != null) {
                 var rnd = Math.random(0);
                 var color = gameColor(player.settings.piece);
                 var pl_ = player, se_ = session;
                 var wplayer = null, bplayer = null, wgameid = null, bgameid = null, wsession = null, bsession = null;
                 if (color == WHITE) {
                    wplayer  = pl_.username;
                    wgameid  = pl_.gameid;
                    wsession = getSession(wplayer); // SESSION_[wplayer];

                    bplayer  = se_.username;
                    bgameid  = match.gameid;
                    bsession = getSession(bplayer); // SESSION_[bplayer];
                 } else
                 if (color == BLACK) {
                    bplayer  = pl_.username;
                    bgameid  = pl_.gameid;
                    bsession = getSession(bplayer); // SESSION_[bplayer];

                    wplayer  = se_.username;
                    wgameid  = match.gameid;
                    wsession = getSession(wplayer); // SESSION_[wplayer];
                 }

                 var gametype = gameType(match.config.gamemin);
                 var variant = variants[match.config.variant - 1].item;
                 var fen = wINITFEN_;
                 if (variant == CHESS960) fen = randomFENchess960();
                 if (variant == HORDE) { fen = wINITFENHORDE_ };

                 var MOLE = genMOLE(variant);
log("match game ...");
log(variant);
log(MOLE);


                 // wsession.rating and bsession.rating are pointers to SESSION_[user].rating.
                 var wgame = { player: wplayer, gameid: wgameid, title: wsession.fide_title, ratesystem: wsession.ratesystem, result: [],
    		  	     socketid: (wplayer == session.username) ? socket.id : player.socketid, rating: wsession.rating };
    
                 var bgame = { player: bplayer, gameid: bgameid, title: bsession.fide_title, ratesystem: bsession.ratesystem, result: [],
    			     socketid: (bplayer == session.username) ? socket.id : player.socketid, rating: bsession.rating };
    
                 var game = { gametoken: player.gametoken, settings: match.config, gametype: gametype, wgame: wgame, bgame: bgame, mole: MOLE, 
    			      round: 1, rematch: false, active: true, abandonedby: null, fen: fen, simul: match.simul, utype: match.utype, 
                              moves: [], elapsed: [] };
    
                 var wuser = { player: wplayer, gameid: wgameid, title: wsession.fide_title, ratesystem: wsession.ratesystem, rating: wsession.rating };
    
                 var buser = { player: bplayer, gameid: bgameid, title: bsession.fide_title, ratesystem: bsession.ratesystem, rating: bsession.rating };
    
                 var their_user = { gametoken: player.gametoken, settings: match.config, gametype: gametype, wgame: wuser, bgame: buser, fen: fen, 
		              round: 1, rematch: false, active: true, abandonedby: null, simul: match.simul, utype: match.utype };

                 var our_user = { gametoken: player.gametoken, settings: match.config, gametype: gametype, wgame: wuser, bgame: buser, fen: fen, 
		              round: 1, rematch: false, active: true, abandonedby: null, simul: match.simul, utype: match.utype };
              
                 if (MOLE != null) {
                    if (player.username == wplayer) {
                       their_user.mole = MOLE.bmole; // set the mole on the black side
                       our_user.mole   = MOLE.wmole; // set the mole on the white side
                    } else {
                       their_user.mole = MOLE.wmole;
                       our_user.mole = MOLE.bmole;
                    }
                 }
    
                 // Tourneys[player.gametoken] = game;
                 setMatch(player.gametoken, game);

                 checkLatency(socket, wgame.socketid, wsession.sessionid, wgame.player);

                 dislodgeFromQueue(player);

                 socket.broadcast.to(player.socketid).emit("play", their_user ); // for the others.
                 socket.emit("play", our_user );  // for myself.
          } else {

                 var now = Date.now(), found = false;
                 var gametoken_ = await bcrypt.hash('rmoo' + now, Math.random());
                 var player = PlayerInQueue[session.username];
                 if (player == null) { 
    		      player = PlayerInQueue[session.username] = [];
                 } else
                 if (player != null) {
                    for (var p in player) {
                      const inqueue = player[p];
                      if (inqueue.gameid == match.gameid) { found = true; break; } // player already offering match in a game board.
                    }
                 }
                 if (!found) {
                    var gametype = gameType(match.config.gamemin);
                    GameQueue.unshift({gametoken: gametoken_,  type: 'match', username: session.username, gameid: match.gameid, 
    			socketid: socket.id, void: false, settings: match.config, gametype: gametype, utype: match.utype }) ;
                    PlayerInQueue[session.username].unshift({ gametoken: gametoken_, gameid: match.gameid, void: false } );
                 }
           }
    }
    
    function rematchGame(socket, session, match) {
    log("we have a rematch ...");
    log(match);
           var srv_game = getMatch(match.gametoken); // Tourneys[match.gametoken];
    log(srv_game);
           if (srv_game != null) {
    log("we got previous play ...");
               if (srv_game.rematch) {
    log("we got rematch offer ...");

                   var fen = wINITFEN_;
                   var variant = variants[srv_game.settings.variant - 1].item;
                   if (variant == CHESS960) fen = randomFENchess960();

                   var MOLE = genMOLE(variant);

                   const wgame      = srv_game.wgame;
                   srv_game.wgame   = srv_game.bgame;
                   srv_game.bgame   = wgame;
                   srv_game.moves   = [];
                   srv_game.elapsed = [];
                   srv_game.fen     = fen;
                   srv_game.rematch = false; // disable rematch until next offer
                   srv_game.round ++;

                   var wsession = getSession(srv_game.wgame.player); // SESSION_[srv_game.wgame.player];
                   var bsession = getSession(srv_game.bgame.player); // SESSION_[srv_game.bgame.player];

                   // if other player originally requested for a new match that is queued prior to rematch
                   // then let us clear the offer from the queue.
                   dislodgeFromQueue({ username: srv_game.wgame.player, gameid: srv_game.wgame.gameid } );
                   dislodgeFromQueue({ username: srv_game.bgame.player, gameid: srv_game.bgame.gameid } );
    log("Check Queue After rematch ...");
    log(GameQueue);
    log(PlayerInQueue);
    
    log("rematch socketids ...");
    log(srv_game.wgame.socketid);
    log(srv_game.bgame.socketid);

                 var wgame_ = { sessionid: wsession.sessionid, player: wsession.username, 
				gameid: srv_game.wgame.gameid, rating: srv_game.wgame.rating, fen: fen }; 
                 var bgame_ = { sessionid: bsession.sessionid, player: bsession.username, 
				gameid: srv_game.bgame.gameid, rating: srv_game.bgame.rating, fen: fen }; 

                 var their_game = { wgame: wgame_, bgame: bgame_ };
                 var our_game   = { wgame: wgame_, bgame: bgame_ };

                 if (match.username == wsession.username) {
                      if (MOLE != null) {
                        their_game.mole = MOLE.bmole;
                        our_game.mole   = MOLE.wmole;
                      }
                      socket.broadcast.to(srv_game.bgame.socketid).emit('rematch', their_game);
                      socket.emit('rematch', our_game);
                 } else {
                      if (MOLE != null) {
                        their_game.mole = MOLE.wmole;
                        our_game.mole   = MOLE.bmole;
                      }
                      socket.broadcast.to(srv_game.wgame.socketid).emit('rematch', their_game);
                      socket.emit('rematch', our_game);
                 }
               } else {
                   // wait for a rematch.
                   srv_game.rematch = true;
               }
          }
    }

    function checkGame(token) {
        const tourney = getMatch(token); // Tourneys[token];
        const simul = getSimul(token); // Simul[token];
        const simul_players = getSimulPlayers(token); // Simul_Players[token];
        return { tourney: tourney, simul: simul, simul_players: simul_players,
                 istourney: (tourney != null), issimul: (simul != null) }
    }

    function isOrphanedGame(chess_, game) {
        // check if we have orphaned games.
        if (chess_.username != null) {
           if (game != null) {
               const tourney = game.tourney;
               const simul   = game.simul;
               if (tourney != null) {
                   if (tourney.wgame.player == chess_.username || tourney.bgame.player == chess_.username) {
                   }
               } else
               if (simul != null) {
                   if (chess_.username == simul.host) {
                   } else {
                      var simul_players = game.simul_players;
                      if (simul_players != null) {
                          var players = simul_players.players;
                          for (var p in players) {
                              var player = players[p];
                              if (chess_.username == player.player) {
                              }
                          }
                      }
                   }
               }
           }
        }
    }

    function revealMole(socket, user) {
       var username = user.username;
       var game = user.game;
       var wgame = game.wgame;
       var bgame = game.bgame;
       var wsession = getSession(wgame.player);
       var bsession = getSession(bgame.player);
       if (wsession != null && wsession.username == username) {
           var psocketid = bsession.socketid;
           var gameid = bgame.gameid;
           socket.broadcast.to(psocketid).emit( "revealmole", 
               { sessionid: bsession.sessionid, username: bsession.username, gameid: gameid, mole: user.mole,
				ismole: user.ismole, istraitor: user.istraitor  }); 
       } else 
       if (bsession != null && bsession.username == username) {
           var psocketid = wsession.socketid;
           var gameid = wgame.gameid;
           socket.broadcast.to(psocketid).emit("revealmole", 
               { sessionid: wsession.sessionid, username: wsession.username, gameid: gameid, mole: user.mole,
 				ismole: user.ismole, istraitor: user.istraitor }); 
       }
    }

/********************** HANDLES DISTRIBUTED SYSTEMS ********************************/
/***** Each function interfaces with proxy server to pool node resources ************/
/****** To look into consistent hashing algorithms ********/

    function getTourneys() {
      return Tourneys;
    }

    function getTourney(token) {
      return Tourneys[token];
    }

    function setTourney(token, record) {
      Tourneys[token] = record;
    }

    function getSimuls(profile) {
log("get simuls");
log(Simul);
      return Simul;
    }

    function getSimul(token) {
      return Simul[token];
    }

    function setSimul(token, record) {
      Simul[token] = record;
    }

    function unsetSimul(token) {
      delete Simul[token];
    }

    function getSimulPlayers(token) {
      return Simul_Players[token];
    }

    function setSimulPlayers(token, record) {
      Simul_Players[token] = record;
    }

    function unsetSimulPlayers(token) {
      delete Simul_Players[token];
    }

    function getSimulHost(host) {
      return Simul_Hosts[host];
    }

    function setSimulHost(host, record) {
      Simul_Hosts[host] = record;
    }

    function unsetSimulHost(host) {
      delete Simul_Hosts[host];
    }

    function getMatch(token) {
      return Matches[token];
    }

    function setMatch(token, record) {
      Matches[token] = record;
    }

/*
    function setForfeitQueue(player, start_date) {
      var f = ForfeitQueue[player.tournament_id];
      if (typeof(f) == "undefined") {
          ForfeitQueue[player.tournament_id] = { players: {} };
      }
      ForfeitQueue[player.tournament_id].players[player.player_id] = { player: player, start_date: start_date };
    }

    function getForfeitQueue(tournament_id) {
      return ForfeitQueue[tournament_id];
    }

    function unsetForfeitQueue(tournament_id) {
      delete ForfeitQueue[tournament_id];
    }
*/

    function getSocket(socketid) {
      return io.sockets.sockets[socketid];
    }

    function getSession(user) {
      return SESSION_[user];
    }

    function setSession(user, record) {
      SESSION_[user] = record;
    }

    function unsetSession(user) {
      delete SESSION_[user];
    }

/******************** A separate server will perform cleanup of rogue / orphaned data ********/
/***** Each function interfaces with proxy server for cleanup  ************/

function garbageCollect(type, timer) {
   if (type == "SESSION") {
       callCleanSession();
   } else 
   if (type == "TOURNEY") {
       callCleanTourney();
   } else 
   if (type == "SIMUL") {
       callCleanSimul();
   } else 
   if (type == "MATCH") {
       callCleanMatch();
   } 
}

function callCleanupSession() {}
function callCleanupTourney() {}
function callCleanupSimul() {}
function callCleanupMatch() {}

/******************** Profiles ***********************************/
    
    function usernamePolicy(un) {
        if (un.length <=5 || un.length > 20) throw { code: 1001, msg: "Username length between 7 and 20.", errid: 1001 };
        if (un[0].toLowerCase().match(/[^a-z]+/)) throw { code: 1002, msg: "Username has to start with an alpha character.", errid: 1001 };
        if (un.toLowerCase().match(/^[^a-z_0-9]$/g)) throw { code: 1003, msg: "Username has to contain only alphanumeric.", errid: 1001 };
    }
    
    function usernamePolicylogin(un) {
        if (un.length <=5 || un.length > 20) throw { code: 1001, msg: "Login failed (3) ...", errid: 1004 };
        if (un[0].toLowerCase().match(/[^a-z]+/)) throw { code: 1002, msg: "Login failed (3) ...", errid: 1004 };
        if (un.toLowerCase().match(/^[^a-z_0-9]$/g)) throw { code: 1003, msg: "Login failed (3) ...", errid: 1004 };
    }
    
    function passwordPolicy(pwd) {
        if (pwd.length <=7 || pwd.length > 12) throw { code: 1001, msg: "Password length between 7 and 12.", errid: 1002 };
    }
    
    function emailPolicy(ml) {
    console.log(ml);
        if (ml.length <=7 || ml.length > 60) throw { code: 1001, msg: "Email length between 7 and 12.", errid: 1003 };
        if (!ml.toLowerCase().match(/^[a-z_\.0-9]+\@[a-z_\.0-9]+$/g)) throw { code: 1002, msg: "Incorrect Email Format.", errid: 1003 };
    }

    function clone(obj) {
       return Object.assign({}, obj );
    }

    function initRating() {
        var ratesystem = { elo: 1500, rmoo: 1500, glicko: 1500, glicko_d: 350, glicko_v: 0.06 }; // initial deviation = 350
        var gametype = { Bullet: clone(ratesystem),  /* bullet */
                         Blitz: clone(ratesystem),  /* blitz */
                         Rapid: clone(ratesystem),  /* rapid */
                         Classical: clone(ratesystem)   /* classical */
                       };
        var rating = { 
                "Standard"         : clone(gametype),  /* standard */
                "Chess960"         : clone(gametype),  /* chess960 */
                "Crazy House"      : clone(gametype),  /* crazyhouse */
                "King Of The Hill" : clone(gametype),  /* kingofthehill */
                "Three Checks"     : clone(gametype),  /* threechecks */
                "Atomic"           : clone(gametype),  /* atomic */
                "Horde"            : clone(gametype),  /* horde */
                "The Mole"         : clone(gametype),  /* themole */
                "The Traitor"      : clone(gametype),  /* thetraitor */
                "AntiChess"        : clone(gametype)   /* antichess */
              };
        return rating;
    }
    
    const signupPlayer = async (profile) => {
        var PDB_ = null;
        try {
            usernamePolicy(profile.unid);
            passwordPolicy(profile.pwid);
            emailPolicy(profile.mlid);
            const PDB_ = await client.connect();
            await PDB_.query(
                "INSERT INTO chessdb.players (player_id, username, password, firstname, lastname, email_address, registered_date )" +
                  " VALUES (nextval('chessdb.player_seq'), $1, $2, $3, $4, $5, to_timestamp($6))",
                  [ profile.unid.toLowerCase(), profile.pwid, profile.fnid, profile.lnid, profile.mlid, UTCtoday()/1000 ]); 
            console.log('inserting ...');
            PDB_.release();
            return { code: 0, msg: 'success!' }
        } catch (error) {
            if (PDB_ != null) PDB_.release();
            const res = { code: error.code, msg: error.msg, errid: error.errid }
            if (error.code == 23505) { res.msg = "Username already exists!"; res.errid = 1001; }; 
            console.log(error.code);
            console.log(error);
            return res;
        } finally {
            // await client.end();
        }
        return null;
    };
    
    function getRating(rating, default_value) {
        return ( rating == null || rating == '' ) ? default_value : rating; 
    }
    
    const updateProfile = async (socket, profile) => {
      var PDB_ = null;
      try {
             usernamePolicy(profile.username);
             emailPolicy(profile.email);
             const PDB_ = await client.connect();
    log("Updating Profile!");
    log(profile);
             await PDB_.query("UPDATE chessdb.players set firstname = $1, lastname = $2, email_address = $3, phone = $4, country = $5, " +
    			   " fide_title=$6, fide_rating = $7, profile = $8 where username = $9 ",
                               [ profile.firstname, profile.lastname, profile.email, profile.phone, 
    				(profile.country == null) ? 0 : profile.country, 
    				(profile.title == null) ? 0 : profile.title, 
    				(profile.rating == '') ? null :  profile.rating, profile.description, profile.username ] );	
             PDB_.release();
             return { code: 0, msg: 'success!' }
          } catch (error) {
             if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid }
              console.log(error);
              console.log(res)
              return res;
          } finally {
         //   await client.end()
          }
    }
    
    const signoutPlayer = async (socket, profile) => {
      try {
                 const session =  inSession(profile.unid);
                 if (session != null && profile.sessionid == session.sessionid) {
    log("dislodging ...");
                    dislodgeFromSession(session);
                 } else { return { code: 1010, msg: "Not logged in yet!", errid: 1 } }
                 return { code: 0, msg: 'success!' }
          } catch (error) {
              const res = { code: error.code, msg: error.msg, errid: error.errid }
              console.log(error);
              console.log(res)
              return res;
          } finally {
         //   await client.end()
          }
    }
    
    const signinPlayer = async (socket, profile, server_reset = false ) => {
        var PDB_ = null;
        try {
              usernamePolicylogin(profile.unid);
              const session =  inSession(profile.unid);
              if (session != null) {
                    const auth = authSession(profile.sessionid, profile.unid);
    log("session ...");
    log(session);
    log("auth session went thru");
    log(auth);
    log("profile ...");
    log(profile);
                  if (auth != null) {
                     throw { code: 1001, msg: "Already logged in ...", errid: 1004 };
                  } 
               }
              PDB_ = await client.connect();
              // as long as we have a cookie session preserved, that's as good as authenticated
              // so let's fetch player profile with no pwd
              var res = null;
              if (server_reset) {
                  res = await PDB_.query('SELECT P.*,  I.imagefile, I.mimetype, I.filename, I.config FROM ' +
                  '(SELECT P.player_id, P.firstname, P.lastname, P.email_address, P.phone, P.fide_title, P.fide_rating, P.country, P.profile ' + 
                  ' FROM chessdb.players P where username = $1) P LEFT OUTER JOIN chessdb.profiles I ON P.player_id = I.profile_id', 
                   [profile.unid.toLowerCase()] );
              } else {
                  res = await PDB_.query('SELECT P.*,  I.imagefile, I.mimetype, I.filename, I.config FROM ' +
                  '(SELECT P.player_id, P.firstname, P.lastname, P.email_address, P.phone, P.fide_title, P.fide_rating, P.country, P.profile ' + 
                  ' FROM chessdb.players P where username = $1 and password = $2) P LEFT OUTER JOIN chessdb.profiles I ON P.player_id = I.profile_id', 
                   [profile.unid.toLowerCase(), profile.pwid] );
              }
      
              if (res.rowCount == 0) throw { code: 1000, msg: "Login failed (2) ...", errid: 1004 };
              const res1 = await PDB_.query('SELECT * FROM chessdb.ratings P where player_id = $1', [ res.rows[0].player_id ]);
              const config_ = CONFIG_[ profile.unid.toLowerCase() ] = res.rows[0].config
    
              var sessionid = null;
              if (server_reset) {
                  sessionid = profile.sessionid; 
              } else {
                  sessionid = await bcrypt.hash(profile.pwid, Math.random());
              }

              const rating = initRating();
    
              if (res1.rowCount > 0) { // update rating
                for (var p in res1.rows) {
                   const row = res1.rows[p];
                   const variant    = recvariants[row.variant - 1];  
                   const timer_type = timer_types[row.timer_type - 1];  
                   const rec = rating[variant][timer_type];
                   rec.rmoo = row.rmoo;
                   rec.elo = row.elo;
                   rec.glicko = row.glicko;
                   rec.glicko_d = row.glicko_d;
                   rec.glicko_v = row.glicko_v;
                }
              }
    
              const session_ = { sessionid: sessionid, username: profile.unid, socketid: socket.id,  ratesystem: 'elo', 
                                 fide_title: res.rows[0].fide_title, fide_rating: res.rows[0].fide_rating, 
                                 country: res.rows[0].country, config: config_, rating: rating }

              const user   = clone(session_);
              user.code    = 0;
              user.msg     = 'success!';
    	      user.photo   = fullName(res.rows[0].player_id, res.rows[0].imagefile, res.rows[0].mimetype);
              delete user.socketid;

              setSession(session_.username, session_); 
              dislodgeFromSession(profile);
              
              PDB_.release();
              return user;
          } catch (error) {
              if (PDB_ != null) PDB_.release();
              const res = { code: error.code, msg: error.msg, errid: error.errid }
              console.log(error);
              console.log(res)
              if ( profile.unid != null ) {
                 const sess_ = getSession(profile.unid); // SESSION_[profile.unid];
                 if (sess_ != null) unsetSession(profile.unid); // delete SESSION_[profile.unid];
              }
              return res;
          } finally {
          }
          return null;
    };
    
    
    function inSession(unid) {
         const session = getSession(unid); // SESSION_[unid];
         if (session != null) {
              return session;
         }
         return null;
    }
    
    function authSession(sessionid, unid) {
         const session = getSession(unid); // SESSION_[unid];
         if (session != null) {
            if (session.sessionid == sessionid) {
              return session;
            }
         }
         return null;
    }


    async function authGuest(token, user, isexpire = false) {
      return await jwt.verify(token, 'rmoo_welcome1', function(err, decode) {
            if (!err) {
                return authSession(token, user); 
            } else {
                if (isexpire) { 
                   if (err instanceof jwt.TokenExpiredError) {
                      unsetSession(user); // delete SESSION_[user];
                      return { expired: true, new_session: createSession(GUEST) };
                   }
                }
                return null;
            }
         });
    }

    function createSession(utype, pwd = 'rmoo_welcome1') {
         var id = '';
         var n1 = ['0','1','2','3','4','5','6','7','8','9'];
         var n2 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
         for (var i=0; i < 5; i++) { var p = Math.round(Math.random() * 9); id = id + n1[p]; }
         for (var i=0; i < 3; i++) { id = id + n2[Math.round(Math.random() * 25)]; }
         var user  = 'Guest.' + id;
         var token = jwt.sign({username: user}, pwd, {expiresIn: 3600 * 10 }); // 1 hours = 3600 seconds
         const rating = initRating();
         return { sessionid: token, username: user, rating: rating, ratesystem: 'elo', fide_title: null, utype: utype };
    }

/****************************** Ratings ****************************/
    
    function ourRating(oA, oB, erating, wrating, brating) {
        const rA = Math.round((erating.rA + wrating.r) / 2);
        const rB = Math.round((erating.rB + brating.r) / 2);
        return { rA: rA, rB: rB, pA: rA - oA, pB: rB - oB };
    
    }
    
    function eloRating(rA, rB, kA = 32, kB = 32, sA = 0.5, sB = 0.5) {
      const pA = 1/ (1 +  Math.pow(10, (rB - rA) / 400 ));
      const pB = 1/ (1 +  Math.pow(10, (rA - rB) / 400 ));
      var r1 = 0, r2 = 0, A_p = 0, B_p = 0; 
       r1 = rA + kA * (sA - pA);
       r2 = rB + kB * (sB - pB);
      r1 = Math.round(r1); r2 = Math.round(r2); 
      return { rA: r1, pA: r1 - rA, rB: r2, pB: r2 - rB };
    }
    
    
    // initial default deviation is 350 and sigma of 0.06
    function glicko2Rating(rA = 1500, dA = 350, sA = 0.06, tRatings, tDeviations, tScores, k = 1 ) {
      const tau = 0.5 /* between 0.3 and 1.2 */ // controls extreme improbable outcomes
      const sfactor = 173.7178;
      const R_ = 1500; /* if player is unrated */
      const epsilon = 0.000001; // tolerance
      const s_ = tScores, points = 0;
    
      const p = function(d) { return d / sfactor; };  // scaled deviation
      var phi = p(dA);  // scaled deviation
    
      const u = function(R) { return (R - R_)/sfactor; }; // rating u
    
    
      const g = function(p) { return 1/ Math.sqrt( 1 + 3 * Math.pow(p, 2) / Math.pow(Math.PI,2) ); };
    
      const E = function(u, u_j, p_j) { return 1/( 1 + Math.exp(-g(p_j) * (u - u_j)));  };
    
      const V = function(u, u_, p_ ) {
                  var v = 0, m = u_.length;
                  for (var j=0; j<m; j++) {
                       v += Math.pow( g(p_[j]), 2) * E( u, u_[j], p_[j]) * ( 1 - E(u, u_[j], p_[j] ) );
                  }
                  return 1/v;
                }
      const D = function(u, u_, p_, s_, v) {
                  var d = 0, m = u_.length;
                  for (var j=0; j<m; j++) {
                     d += g(p_[j]) * ( s_[j] - E(u, u_[j], p_[j]) );
                  }
                  return v * d;
                }
    
      const f = function(x, d, v, p, tau) {
                return    ( Math.exp(x) * ( Math.pow(d, 2) - Math.pow(p, 2) - v - Math.exp(x)))  /
                          ( 2 * Math.pow( Math.pow(p, 2) + v + Math.exp(x) , 2)) - ( x - a ) / Math.pow(tau, 2);
                }
    
      const myu  = u(rA), u_ = [], p_ = [];
    
      for (var i in tRatings) { u_.push(u(tRatings[i])); }
      for (var i in tDeviations) { p_.push(p(tDeviations[i])); }
    
      var v = V(myu, u_, p_);
    
      const delta = D(myu, u_, p_, s_, v);
    
      var A = a = Math.log(Math.pow(sA, 2));
      var B = a - k * tau;
      var C = null;
    
    
      /*** Begin Iteration ***/
      if (Math.pow(delta,2) > Math.pow(phi,2) + v) {
         B = Math.log(Math.pow(delta, 2) - Math.pow(phi,2) - v);
      }  else {
          k = 1;
          while (f(B, delta, v, phi, tau) < 0) {
             k = k + 1;
             B = a - k * tau;
          }
      }
    
      var fA = f(A, delta, v, phi, tau);
      var fB = f(B, delta, v, phi, tau);
      var fC = null;
    
      while (Math.abs(B - A) > epsilon) {
           C = A +  (A - B) * fA  / ( fB - fA );
           fC = f(C, delta, v, phi, tau);
           if (fC * fB <= 0) {
               A = B; fA = fB;
           } else {
               fA = fA / 2;
           }
           B = C; fB = fC;
       }
    
       const sA_ = Math.exp(A/2);
       const mu = function(u, phi_, u_, p_, s_, v) {
                    var d = 0, m = u_.length;
                    for (var j=0; j<m; j++) {
                       d += g(p_[j]) * ( s_[j] - E(u, u_[j], p_[j]) );
                    }
                    return u + Math.pow(phi_,2) * d;
                  }
    
       const nphi_ = Math.sqrt( Math.pow(phi,2) + Math.pow(sA_, 2));
       const phi_ = 1 / Math.sqrt( 1/ Math.pow(nphi_,2) + 1/v );
       const myu_ = mu(myu, phi_, u_, p_, s_, v);
    
       const rA_ = sfactor * myu_ + R_;
       const dA_ = sfactor * phi_;
    
       return { r: Math.round(rA_), d: dA_, v: sA_, p: Math.round(rA_) - rA };
    }
    
    
 /********************** SCHEDULE TRACKER FOR TOURNAMENTS ****************************/ 

    function daylocal(tdate) {
        return new Date(tdate).toLocaleString('en-us',
             { weekday:"short", year:"numeric", month:"short", day:"numeric",
             hour: "numeric", minute: "numeric"});
    }

    function daymsg(start_date, duration) { // duration in minutes
       var msg = '';
       var today = UTCtoday(); 
       var infuture = start_date/1;  /* already in UTC from storage */ 
       var ndays = 24 * 60 * 60 * 1000;
       var nhrs = 60 * 60 * 1000;
       var nmins = 60 * 1000;
       var nsecs = 1000;
       var days = Math.floor((infuture - today) / ndays);
       var weeks = Math.floor(days / 7);
       var extra_days = (infuture - today) % ndays;
       var hrs = Math.floor(extra_days / nhrs);
       var extra_hrs = extra_days % nhrs;
       var mins = Math.floor(extra_hrs / nmins);
       var extra_mins = extra_hrs % nmins;
       var secs  = Math.ceil(extra_mins / nsecs);
       var dmins = duration * 60 * 1000;
       var end_date = infuture + dmins; // use LOCALdate(end_date) and LOCALdate(start_date) outside this function to display date.
       var endsin = Math.ceil(((infuture + dmins) - today) / nmins);
       var stillrunning = (infuture <= today) && (today <= (infuture + dmins));
       return { duration: duration, startsin: endsin - duration, endsin: endsin, stillrunning: stillrunning, ago: (extra_mins < 0),
               dates: { weeks: weeks, days: days, hrs: hrs, mins: mins, secs: secs }, end_date: end_date };
    }


    function createSamplePlayers(len) {
         var online = [];
         for (var i=1; i <= len;  i++) {
              online.push({ tournament_id: 10, player_id: i, username: 'user_' + i});
         }
         if (len % 2) online.push({ tournament_id: 10, player_id: -1, username: 'bye'});
         return online;
    }

    function initPositions(players) {
        var len = players.length;
        var position = {};
        for (var i=0; i < len; i++) {
           position[players[i].player_id] = { white: 0, black: 0, color: '' };
        }
        return position;
    }

    /* We use Berger/Fide Rule Table method  and not Crenshaw-Berger/USCF.
        - The first group of  players are arranged in ascending order.
        - The second group of players are arranged in descending order. then we pair them.
        - We then keep the first player in the second group as pivot (constant).
        - We then rotate the rest of the players in clockwise direction.
        - Switch the first player in the second group  (the pivot) to take the black position for odd rounds.
        - Now re-index so that the pivot takes alternate position in each round.
       If a player withdraws less than 50% of his games, a reversal happens for even number of players.
    */
    function RoundRobinSystem(players) {
        var len = players.length;
        var isbye = (len % 2);
        var home  = Array.from({length: len / 2}, (v, k) => k+1);
        var guest  = Array.from({length: len / 2}, (v, k) => k+len / 2 + 1);
        var oplan = {}, pivot = null;
        guest.reverse();
        var position = initPositions(players);

        for (var i=0; i < len - 1; i++) {
           var idx = 0;
           const plan = [];
           for (var j=0; j < guest.length; j++) {
              var m = home[j];
              var n = guest[j];
              var home_  = players[m - 1].player_id;
              var guest_ = players[n - 1].player_id;
              var white = home_ ;
              var black = guest_;

              if (i >= len / 2) {
                 white = (j == 0) ? guest_ : home_; /* switch position of the pivot */
                 black = (j == 0) ? home_ : guest_; /* switch position of the pivot */
                 idx = (i - len/2) * 2 + 1; /* re-index odd rounds */
              } else {
                 idx = i * 2; /* re-index even rounds */
              }
              plan.push({ white: white, black: black, gameover: false, start_date: null, moves: [], elapsed: [] });
              position[white].white ++;
              position[black].black ++;
           }
           oplan[idx] = plan;
           /** rotate all players except the pivot in clockwise direction **/
           pivot = guest.shift();
           guest.unshift(home.shift());
           home.push(guest.pop());
           guest.unshift(pivot);
        }
        return { position: position, plan: oplan, isbye: isbye };
    }

    function switchPosition(game) {
        var a = game.white;
        game.white = game.black;
        game.black = a;
    }

    function withdrawPlayer(player, where, pairing, players ) {
        var len = Object.keys(pairing.position).length;
        var withdraw = players[player - 1].player_id;
        // nullify withdrawn player.
        for (var i = where; i < len - 1;i++) {
              var games = pairing.plan[i];
              for (var p in games) {
                 var game = games[p];
                 if (game.white == withdraw) game.white = null;
                 if (game.black == withdraw) game.black = null;
              }
        }
    }

    function checkImbalance(withdraw, where, pairing, players) {
        var position = initPositions(players);
        var len = Object.keys(position).length;

        for (var i=0; i < len - 1; i++) {
            var games = pairing.plan[i];
            for (var p in games) {
                 var game = games[p];
                 if (game.white == null || game.black == null) {
                    if (game.white == null && game.black == null) {
                        continue;
                    }
                    if (game.white == null) position[game.black].color += '. ';
                    if (game.black == null) position[game.white].color += '. ';
                    if (withdraw != null) position[withdraw].color += '. ';
                    continue;
                 }
                 position[game.white].white ++;
                 position[game.white].color += 'w ';
                 position[game.black].black ++;
                 position[game.black].color += 'b ';
            }
        }
        return position;
    }

    function checkQuorum(withdraw, where, pairing, players) {
// log("checkQuorum");
        var position = checkImbalance(withdraw, where, pairing, players);
        var len = Object.keys(position).length;
// log("prior to switch");
// log(position);
        for (var x = 0; x < 2; x++)
        for (var p in position) {
          var player = position[p];
          var diff = player.white - player.black;
          if (diff >= 2) {
              for (var i = where; i < len - 1;i++) {
                 var games = pairing.plan[i];
                  for (var g in games) {
                      var game = games[g];
                      if (game.white == p) {
//  log("switch game for " + p +  " in round " + i + "..."); log(game);
                          var other = position[game.black];
                          if (other == null) continue;
                          var diff = other.black - other.white;
                          if (diff > 0) {
                             var squeeze = other.color.replace(/[\.\ ]/g, '');
                             var color = Array.from(squeeze);
                             color[i] = 'w';
                             var squeezed = color.join('');
                             var morecolors = squeezed.match('www');
                             if (morecolors == null) {
// log("switched ...");
                                switchPosition(game);
                             }
                          }
                      }
                  }
              }
          }
          var diff = player.white - player.black;
          if (diff <= -2) {
              for (var i = where; i < len - 1;i++) {
                 var games = pairing.plan[i];
                  for (var g in games) {
                      var game = games[g];
                      if (game.black == p) {
// log("switch game for " + p +  " in round " + i + "..."); log(game);
                          var other = position[game.white];
                          if (other == null) continue;
                          var diff = other.white - other.black;
                          if (diff > 0) {
                             var squeeze = other.color.replace(/[\.\ ]/g, '');
                             var color = Array.from(squeeze);
                             color[i] = 'b';
                             var squeezed = color.join('');
                             var morecolors = squeezed.match('bbb');
                             if (morecolors == null) {
// log("switched ...");
                                switchPosition(game);
                             }
                          }
                      }
                  }
              }
        }
        var squeeze = player.color.replace(/[\.\ ]/g, '');
        if (squeeze.match('www') && 0) {
// log("too many www");
              var mt = squeeze.match('www');
              var start = parseInt(mt.index > where ? mt.index : where);
              var end = (start + 3) > len - 1 ? len - 1 : start + 3;
// log("start - end ");
// log(start + 3);
// log(start); log(where);
// log(end); log(len - 1);
              for (var i = start; i < end;i++) {
                 var games = pairing.plan[i];
// log("next round: " + i );
                  for (var g in games) {
                      var game = games[g];
                      if (game.white == p) {
// log("switch game for " + p +  " in round " + i + "..."); log(game);
                          var other = position[game.white];
                          if (other == null) continue;
                          var diff = other.black - other.white;
                          if (diff > 0) {
                             var squeeze = other.color.replace(/[\.\ ]/g, '');
                             var color = Array.from(squeeze);
                             color[i] = 'b';
                             var squeezed = color.join('');
                             var morecolors = squeezed.match('www');
                             if (morecolors == null) {
// log("switched ...");
                                switchPosition(game);
                             }
                          }
                      }
                  }
              }
        }
        var squeeze = player.color.replace(/[\.\ ]/g, '');
        if (squeeze.match('bbb') && 0) {
// log("too many bbb");
              var mt = squeeze.match('bbb');
              var start = parseInt(mt.index > where ? mt.index : where);
              var end = (start + 3) > len - 1 ? len - 1 : start + 3;
// log("start - end ");
// log(start + 3);
// log(start); log(where);
// log(end); log(len - 1);
              for (var i = start; i < end;i++) {
                 var games = pairing.plan[i];
// log("next round: " + i );
                  for (var g in games) {
                      var game = games[g];
                      if (game.black == p) {
//   log("switch game for " + p +  " in round " + i + "..."); log(game);
                          var other = position[game.white];
                          if (other == null) continue;
                          var diff = other.white - other.black;
                          if (diff > 0) {
                             var squeeze = other.color.replace(/[\.\ ]/g, '');
                             var color = Array.from(squeeze);
                             color[i] = 'b';
                             var squeezed = color.join('');
                             var morecolors = squeezed.match('bbb');
                             if (morecolors == null) {
// log("switched ...");
                                switchPosition(game);
                             }
                          }
                      }
                  }
              }
        }
      }
      position = checkImbalance(withdraw, where, pairing, players);
      return position;
    }

    function notifyToStartGames(tourney, games, action) {
        var round = tourney.currentround;
        var seat = tourney.players; 
        for (var g in games) {
            var game = games[g];
log("notify player");
// log("game");
// log(game);
// log(seat);
            var white = seat[game.white];
            var black = seat[game.black];

            if (white != null && black != null) {
log("note got here 1 ...");
                var wsocket = white.socket;
                var bsocket = black.socket;
                var wsession = getSession(white.player.username);
                var bsession = getSession(black.player.username);
                if (wsession != null &&  bsession != null) {
log("note got here 2 ...");
log("note got here 4 ...");
                   var MOLE = genMOLE(tourney.variant);
                   var bgame = { player: bsession.username, gameid: game.bgameid, rating: bsession.rating, title: bsession.title };
                   var wgame = { player: wsession.username, gameid: game.wgameid, rating: wsession.rating, title: wsession.title };
                   if (game.wgameid == null) // see ackTournament() for this setting
                   wsocket.emit(action, { sessionid: wsession.sessionid, username: wsession.username, simul: false,
		                	  settings: { variant: tourney.variant + 1, timer: tourney.timer, inc: tourney.increment },
                                          gametype: gameType(tourney.timer), gametoken: white.player.tournament_id, 
                                          round: round, wgame: wgame, bgame: bgame, mole: MOLE.wmole } );
                   if (game.bgameid == null) // see ackTournament() for this setting
                   bsocket.emit(action, { sessionid: bsession.sessionid, username: bsession.username, simul: false,
					  settings: { variant: tourney.variant + 1, timer: tourney.timer, inc: tourney.increment },
                                          gametype: gameType(tourney.timer), gametoken: black.player.tournament_id, 
                                          round: round, wgame: wgame, bgame: bgame, mole: MOLE.bmole } );
                } else 
                if (wsession != null) {
log("note got here 5 ...");
log("note got here 7 ...");
                   var MOLE = null;
                   var wgame = { player: wsession.username, gameid: game.wgameid, rating: wsession.rating, title: wsession.title };
                   var bgame = { player: black.player.username, gameid: null, rating: initRating(), title: null, missing: true, 
					forfeited: black.forfeited }; // black is missing
                   var game_ = { sessionid: wsession.sessionid, username: wsession.username, simul: false, forfeit_policy: tourney.forfeit_policy,
                                          settings: { variant: tourney.variant + 1, timer: tourney.timer, inc: tourney.increment },
                                          gametype: gameType(tourney.timer), gametoken: white.player.tournament_id,
                                          round: round, wgame: wgame, bgame: bgame };
                   if (action == "starttournament") {
                      wsocket.emit(action, game_);
                   } else
                   if (game.wgameid == null) { // see ackTournament() for this setting

                      game_.fen = wINITFEN_;
                      const variant  = recvariants[game_.settings.variant - 1];
                      if (variant == CHESS960) { game_.fen = randomFENchess960(); };
                      if (variant == HORDE) { game_.fen = wINITFENHORDE_; };

                      MOLE = genMOLE(tourney.variant);
                      game_.mole = MOLE.wmole;

                      wsocket.emit("ongoingtournament", game_);
                   } else
                   if (!game.gameover && black.forfeited) { // see getOnlinePlayer() for this setting
                      game.gameover = true;
                      wsocket.emit("forfeitintournament", game_);
                      updatePairing(tourney);
                   }
                } else 
                if (bsession != null) {
log("note got here 8 ...");
log("note got here 10 ...");
log(game.bgameid);
                   var MOLE = null;
                   var bgame = { player: bsession.username, gameid: game.bgameid, rating: bsession.rating, title: bsession.title };
                   var wgame = { player: white.player.username, gameid: null, rating: initRating(), title: null, missing: true,
					forfeited: white.forfeited }; // white is missing
                   var game_ =  { sessionid: bsession.sessionid, username: bsession.username, simul: false,
                                          settings: { variant: tourney.variant + 1, timer: tourney.timer, inc: tourney.increment },
                                          gametype: gameType(tourney.timer), gametoken: black.player.tournament_id,
                                          round: round, wgame: wgame, bgame: bgame }; 
                   if (action == "starttournament") {
                      bsocket.emit(action, game_);
                   } else
                   if (game.bgameid == null) { // see ackTournament for this setting

                      game_.fen = wINITFEN_;
                      const variant  = recvariants[game_.settings.variant - 1];
                      if (variant == CHESS960) { game_.fen = randomFENchess960(); };
                      if (variant == HORDE) { game_.fen = wINITFENHORDE_; };

                      MOLE = genMOLE(tourney.variant);
                      game_.mole = MOLE.bmole;

                      bsocket.emit("ongoingtournament", game_); 
                   } else
                   if (!game.gameover && white.forfeited) { // see getOnlinePlayer() for this setting
                      game.gameover = true;
                      bsocket.emit("forfeitintournament", game_);
                      updatePairing(tourney);
log("pri ...");
                   }
                } else {
log("A player not available .... cancel tournament? .... ");
                }
            }
        }
    }

    function setAssignments(tourney, players) {
        var tagnumber = 0;
        var seats = {}
        if (tourney.players == null) {
           for (var p in players) { // registered users
              var player = players[p];
              tagnumber ++;
              seats[tagnumber] = player;
           }
        }
        return seats;
    }

    function setRound(tourney) {
        var plan = tourney.pairing.plan;
        var seat = tourney.players;
        if (tourney.rounds == null)  { tourney.rounds = {}; tourney.attendance = 0 };
        var completed_games = 0, cnt_games = 0;
        for (var p in plan) {
           var games = plan[p];
           var found = false;
           cnt_games ++;
           if (tourney.rounds[p] == null) {
                 tourney.rounds[p] = { status: 0, start_date: UTCtoday(), end_date: null, completed: false }
           }
           // check attendance. if forfeited, then game over
           var full_attendance = 0, attendance = 0, gameover = 0, total = 0;
           for (var g in games) {
              var game = games[g];
              var white = seat[game.white];
              var black = seat[game.black];
              if (white != null && black != null)  { full_attendance++; found = true; }; 
              if (white != null && !white.forfeited) attendance ++;
              if (black != null && !black.forfeited) attendance ++;
              if (game.gameover != null && game.gameover) gameover ++;
              total++;
           }
           if (gameover == full_attendance) {
log("game over ... :" + gameover + " at round " + p);
             completed_games ++;
             // go to next round ...
             if (!tourney.rounds[p].completed)  {
                tourney.rounds[p].end_date = UTCtoday();
                updatePairing(tourney);
             }
             tourney.rounds[p].completed = true;
             continue;
           } else
           if (found) {  // in next round, if we find a matching game, then we should be good to start 
log("found ...");
log(games);
log("currentround:" + tourney.currentround);
log("currentround:" + p);
              tourney.attendance = attendance;
              if (tourney.rounds[p].status == 0) {
log("coz it's not getting here 1");
                  tourney.rounds[p].status = 1;
                  tourney.currentround = p;
                  notifyToStartGames(tourney, games,  "starttournament"); // notify for readiness, delay game for 10 seconds.
                  updatePairing(tourney);
              } else 
              if (tourney.rounds[p].status == 1 && tourney.currentround == p) { // now, begin the game.
log("coz it's not getting here 2 ");
                  notifyToStartGames(tourney, games, "ongoingtournament");
              }
              setTourney(tourney.tournament_id, tourney); // update tourney due to notify updates.
              return;
           } else 
           if (full_attendance == 0) { 
log("round: " + p + " has no players 1 ...");
           } else {
log("round: " + p + " has no players 2 ...");
           }
            
        }
        if (cnt_games == completed_games ) {
log("tournament completed ....");
            endTournament(tourney);
            notifyEndTournament(tourney);
            setTourney(tourney.tournament_id, tourney); // update tourney due to notify updates.
            
        }
    }

    function notifyEndTournament(tourney) {
        for (var p in tourney.players) {
            var who_ = tourney.players[p];
            var socket = who_.socket;
            var session = getSession(who_.player.username);
            if (session != null) 
            socket.emit('donetournament', { sessionid: session.sessionid, username: session.username, gametoken: tourney.gametoken, 
				score: who_.score, result: who_.result });
        }
    }

    async function ackTournament(socket, session, profile) {
        var tourney_id = profile.gametoken;
        var tourney = getTourney(tourney_id);
        var seat = tourney.players;
        if (tourney != null) {
           var games = tourney.pairing.plan[profile.round];
           if (games != null) {
              var update = false;
              for (var g in games) {
                 var game = games[g];
                 var white = seat[game.white];
                 var black = seat[game.black];
                 if (white != null && black != null) {
                    // update the system that the two players are now on the board.
                    if (white.player.username == profile.username) { game.wgameid = profile.gameid; update = true; };
                    if (black.player.username == profile.username) { game.bgameid = profile.gameid; update = true; }; 
                    if (game.start_date == null) game.start_date = UTCtoday();
                 }
              }
              if (update) setTourney(tourney.tournament_id, tourney);
           }
        }
    }

    async function organizeTournament(tourney) {

        if (tourney.players == null) { 
            var resp = await getRegisteredPlayers(tourney);
            if (resp.code == 0) {
                var len = resp.players.length;
                var players = createSamplePlayers(len);

                if (tourney.pairing == null) { 
                    var pairing = RoundRobinSystem(players);
   		    tourney.pairing = pairing; 
                    updatePairing(tourney);
                };

                tourney.players = setAssignments(tourney, resp.players);
            }
        } 
log("---------------------");
log("tournament id");
for (var p in tourney.players) {
log("p:" + p + " - " + tourney.players[p].player.username);
}
        await getOnlinePlayers(tourney);
        setRound(tourney);
        setTourney(tourney.tournament_id, tourney);
    }

    async function checkTournament(reach_count = false) {
       if (server_restart || reach_count) {
            var record = getTournaments(null, true ); // refresh every minute.
            record.then((resp) => {
                if (resp.code == 0) {
                     const tourneys = resp.tourneys;
log("tourneys from DB ...");
log(tourneys);
                     for (var p in tourneys) {
                         const tourney = tourneys[p];
                         const tourney_ = getTourney(tourney.tournament_id);
                         if (tourney_ != null) {
                            tourney_.status = tourney.status;
                            tourney_.joinedplayers = parseInt(tourney.joinedplayers);
log("update cache");
                            setTourney(tourney_.tournament_id, tourney_); // we're assuming here that source is distributed.
                         } else {
log("new - add to cache");
                            setTourney(tourney.tournament_id, tourney); // we're assuming here that source is distributed.
                         }
log(" - name: " + tourney.tournament_name);
log(" - id: " + tourney.tournament_id);
log(" - joined: " + tourney.joinedplayers);
log(" - status: " + tourney.status);
                     } 
                     server_restart = false;
                }
            });
       } else {
            const tourneys = getTourneys();
log("tourneys in cache ...");
            for (var p in tourneys) {
                const tourney = tourneys[p];
                const date = daymsg(tourney.start_date, tourney.duration);
/*
log(" ------ name: " + tourney.tournament_name);
log(" - start_date: " + LOCALdate(tourney.start_date));
log(" - end_date : " + LOCALdate(date.end_date));
log(" - id: " + tourney.tournament_id);
log(" - joined: " + tourney.joinedplayers);
log(" - status: " + tourney.status);
log(" - startsin: " + date.startsin);
log(" - endsin: " + date.endsin);
log(" - stillrunning: " + date.stillrunning);
log(" - ago: " + date.ago);
*/

                if (tourney.status == 3) {
                  continue;  // this tournament has ended.
                } else
                if (date.startsin == 0 && tourney.status == 0) {
 log("start tournament ...");
 log(" tourney: " + tourney.tournament_id);
                    var res = await startTournament(tourney);
                } else
                if (tourney.status == 1 && (date.startsin == 0 || date.stillrunning)) { /* tourney started */
 log("now let's organize ...");
                    await organizeTournament(tourney);
                } else
                if (tourney.status == 0 && date.ago  && date.stillrunning) { /* this happens when server is down around the time tourney should start */
 log("late in starting so we give it a new start date ...");
                    var res = await startTournament(tourney); // we start the tournament but adjust the start date.
                } else {
 log("no action ...");
                }
            }
       }
    }

    var reach_count = 0;
    function runJob() {
        reach_count += 1;
log(reach_count);
        if (reach_count > 6) reach_count = 0;  
           checkTournament(reach_count == 6); // recycle tourney every 1 minute
log("sessions ... reach count: " + reach_count);
for (var x in SESSION_) {
log(" - " + SESSION_[x].username);
}
    }

    function runSchedule() {  // executed inside io.connection
       setInterval(runJob, SCHED_TIMER);
    }
 
    // runSchedule();

 /*************************************************************************************/
