const stockfish = require('stockfish');
const engine = await stockfish();
engine.postMessage('setoption name Contempt value 30');
