const peg = require("pegjs");
const jsondb = require('simple-json-db');
const fs = require("fs");

const db = new jsondb('./storage/chess_store.json');

db.set("ray1", "mon1");
db.set("ray2", "mon2");
db.set("ray3", "mon3");
db.set("ray4", "mon4");

return;

try {  
    var grammar = fs.readFileSync('jscripts/pgn_grammar.pegjs', 'utf8');
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
                if (we >=2200 && be >=2200) Games.push(game.detail)
             }
        }
    return Games;
}

try {  
     var pgn = fs.readFileSync('pgn/BenkoGambit.pgn', 'utf8');
   
    pgn = pgn.toString();
} catch(e) {
    console.log('Error:', e.stack);
}

var parser = peg.generate(grammar);
// pgn = "1.Nf6+ gxf6 2.Qe2 Ke1 1-0"
// pgn   = "1.Nf6+ gxf6 2.Bxf7# 1-0";
// pgn = "1.e4 c5 2.Nf3 d6 3.d3 Nc6 4.Be2 h6 5.O-O Nf6 6.a3 g5 7.Nc3 g4 8.Nd2 Bg7 9.b4 h5 10.bxc5 Nd7 11.Rb1 Bxc3 12.cxd6 exd6 13.Nb3 Nc5 14.d4 Nxb3 15.cxb3 Bxd4 16.b4 Qf6 17.Qd3 h4 18.b5 Ne5 19.Qb3 20.hxg3 Be6 21.Qb4 hxg3 22.b6 gxf2+ 23.Rxf2 Qxf2# 0-1 ";
// pgn = "20.hxg3 Be6 21.Qb4 hxg3 22.b6 gxf2+ 23.Rxf2 Qxf2# 0-1 ";
pgn = "1.e4 c5 2.Nf3 d6 3.d3 Nc6 4.Be2 h6 5.O-O Nf6 6.a3 g5 7.Nc3 g4 8.Nd2 Bg7 9.b4 h5 10.bxc5 Nd7 11.Rb1 Bxc3 12.cxd6 exd6 13.Nb3 Nc5 14.d4 Nxb3 15.cxb3 Bxd4 16.b4 Qf6 17.Qd3 h4 0-1"
//pgn = "1.Nf6+ gxf6 2.Bxf7# 1-0"
// pgn = "1.d4 d6 2.c4 g6 3.Nc3 Bg7 4.Nf3 Nf6 5.h3 O-O 6.e4 a6 7.Be3 b5 8.cxb5 axb5 9.Bxb5 Bb7 10.Qc2 Nbd7 11.d5 Nb6 12.O-O Ra5 13.a4 c6 14.dxc6 Bxc6 15.Bxc6 Qc7 16.Nb5 Qb8 17.b4 Ra8 18.Bxa8 Qxa8 19.Bxb6 Nbd7 20.Nc3 h5 21.Ra3 Qb8 22.Qb3 0-1";
// pgn = "1.e4 e5 2.Nf3 Nc6 3.d3 d5 4.Be2 dxe4 5.dxe4 Qxd1+ 6.Bxd1 Nf6 7.O-O Nxe4 8.c3 Nd6 9.Be3 Bd7 10.Nbd2 f6 11.Bc2 O-O-O 12.Nb3 Nc4 13.Bc1 Be6 14.Rd1 Rxd1+ 15.Bxd1 Be7 16.Kf1 b6 17.h3 Kb7 18.Nbd2 Nd6 19.Be2 g5 20.Nh2 f5 21.Nb3 h5 22.Bd2 a5 23.Re1 a4 24.Nc1 g4 25.Nd3 Ne4 26.Bc1 Bxa2 27.Bd1 Bc4 28.Bc2 Nc5 29.Rd1 e4 30.hxg4 exd3 31.Bxd3 Nxd3 32.Rxd3 Ne5 33.gxh5 Nxd3 34.Kg1 Nxc1 35.Nf1 36.Kh2 Rxh5# 0-1 ";
pgn = "33.gxh5 Nxd3 34.Kg1 Nxc1 35.Nf1 36.Kh2 Rxh5# 0-1";
console.log(pgn);
p = parser.parse(pgn); 
// games = simplify(p);
console.log(p);
console.log(JSON.stringify(p));
