    WHITE = 97;
    BLACK = 98;
    function log(msg) {
      console.log(msg);
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
              plan.push({ white: white, black: black });
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

    function organizeTournament(nplayers, withdraw, where) {

        /*** create sample players for debugging ***/
        var players = createSamplePlayers(nplayers);
// log("synthetic players ...");
// log(players);

        if (players) {
           var pairing = RoundRobinSystem(players);
           // var pairing = SwissSystem(players);
// log("pairing ...");
// log(pairing.plan);
//           var pos = checkQuorum(withdraw, where, pairing, players);
// log("position");
// log(pos);
           withdrawPlayer(withdraw, where, pairing, players);
           withdrawPlayer(1, where, pairing, players);
 log("pairing ...");
 log(pairing.plan);
           var pos = checkQuorum(withdraw, where, pairing, players);
 log("position after withdrawal");
 log(pos);
        }
    }


  
  var nplayers = 8, withdraw = 3, location = 2;
  process.argv.forEach(function (val, index, array) {
  console.log(index + ': ' + val);
     if (index == 2) nplayers = val;
     if (index == 3) withdraw = val;
     if (index == 4) location = val;
  });
  organizeTournament(nplayers, withdraw, location);
  log(nplayers);
  log(withdraw);
  log(location);

