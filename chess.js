const stockfish = require('stockfish')
const engine = stockfish();

async function run_engine(){


        await engine().then((sf) => {
             mengine = sf
             mengine.onmessage = function (message) {
		  console.log("received: " + message);
              }
        });
		
	mengine.postMessage('setoption name Contempt value 30');
	mengine.postMessage('setoption name Skill Level value 20');
	mengine.postMessage('ucinewgame');
	mengine.postMessage('isready');

}

run_engine();

// engine.postMessage('setoption name Contempt value 30');

// stockfishes[id].postMessage('setoption name Skill Level value 20');
// stockfishes[id].postMessage('ucinewgame');
// stockfishes[id].postMessage('isready');

// console.log(sfish.postMessage);
//sfish.send = sfish.postMessage;
//sfish.send("setoption name UCI_AnalyseMode value true");
//sfish.send("setoption name MultiPV value 5");
//sfish.send("isready");
//sfish.send("ucinewgame");
//sfish.send("position fen 8/8/4Rp2/5P2/1PP1pkP1/7P/1P1r4/7K b - - 0 40");
//sfish.send("go movetime 1000");
