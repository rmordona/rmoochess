    WHITE = 97;
    BLACK = 98;
    function log(msg) {
      console.log(msg);
     }

    function createSamplePlayers(len_) {
         var len = (len_ % 2) ? len_ + 1 : len_;
         var online = [];
         for (var i=1; i <= len_;  i++) {
              online.push({ tournament_id: 10, player_id: i, username: 'user_' + i});
         }
         return online;
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
        if (isbye) {
            players.push({ tournament_id: 10, player_id: null, username: 'bye' });
            len ++;
        }
        var home  = Array.from({length: len / 2}, (v, k) => k+1);
        var guest  = Array.from({length: len / 2}, (v, k) => k+len / 2 + 1);
        var oplan = {}, pivot = null;
        guest.reverse();
        var position = {}
        for (var i=0; i < len; i++) {
           position[players[i].username] = { white: 0, black: 0, color: '' };
        }
        for (var i=0; i < len - 1; i++) {
           var idx = 0;
           const plan = [];
           for (var j=0; j < guest.length; j++) {
              var m = home[j];
              var n = guest[j];
              var home_  = players[m - 1].username;
              var guest_ = players[n - 1].username;
              var white = home_; // (j % 2) ? home_ : guest_; // home_
              var black = guest_ ; // (j % 2) ? guest_: home_; // guest_

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

    function RoundRobinSystem(players) {
        for (var i=0; i < len - 1; i++) {
    }

    function reversal(player, pairing, players ) {
        var len = Object.keys(pairing.position).length;
        var lim = 2; // (len == 4) ? 2 : len - 4;
        var withdraw = players[player - 1].username;
        var position = {}
        for (var i=0; i < len; i++) {
           position[players[i].username] = { white: 0, black: 0, color: '' };
        }
        // nullify withdrawn player.
        for (var i=lim; i < len - 1;i++) {
log(i);
log(withdraw);
log(pairing.plan[i]);
              var games = pairing.plan[i];
              for (var p in games) {
                 var game = games[p];
                 if (game.white == withdraw) game.white = null;
                 if (game.black == withdraw) game.black = null;
              }
        } 
        // calculate positions
        for (var i=0; i < len - 1;i++) {
            var games = pairing.plan[i];
            for (var p in games) {
                 var game = games[p];
                 if (game.white == null || game.black == null) {
                    if (game.white == null) position[game.black].color += '. ';
                    if (game.black == null) position[game.white].color += '. ';
                    position[withdraw].color += '. '; 
                    continue;
                 }
                 position[game.white].white ++; 
                 position[game.white].color += 'w ';
                 position[game.black].black ++;
                 position[game.black].color += 'b ';
            }
        }
log("after reversal ...");
log(pairing.plan);
log(position);
        for (var p in position) {
            var player = position[p];
            var round = player.color.replace(/\.\ /g,'').replace(/\ /g,''); 
            if (round.match('www') || round.match('bbb')) {
                log(p + ' ' + round);
            }
        }
    }

    function removePlayer(who, players) {
       var len = players.length;
       const new_p = [];
       for (var i = 0; i < len; i ++) {
         if (who-1 != i) new_p.push(players[i]);
       }
       log(new_p);
       return new_p;
    }

    function initPositions(players) {
        var len = players.length;
        var position = {}
        for (var i=0; i < len; i++) {
           position[players[i].username] = { white: 0, black: 0, color: '' };
        }
        return position;
    }

    function checkQuorum(rounds, position, pairing, players) {
        var rounds_ = rounds;
        for (var i=0; i < rounds_;i++) {
            var games = pairing.plan[i];
            for (var p in games) {
                 var game = games[p];
                 position[game.white].white ++; 
                 position[game.white].color += 'w ';
                 position[game.black].black ++;
                 position[game.black].color += 'b ';
            }
        }
        return position;
    }

    function organizeTournament() {

        /*** create sample players for debugging ***/
        var players = createSamplePlayers(8);
log("synthetic players ...");
log(players);

        if (players) {
           var position = initPositions(players);
           var pairing = RoundRobinSystem(players);
           // var pairing = SwissSystem(players);
log("pairing ...");
log(pairing.plan);
           var pos = checkQuorum(8, position, pairing, players);
log("position");
log(pos);
           // assume a player withdraws. remove from the playerslist.
           // var new_players = removePlayer(3, players);
           // var pairing = RoundRobinSystem(new_players);

           // var position = initPositions(new_players);
           // var pos = checkQuorum(8, position, pairing, new_players);
           
  //         if (!pairing.isbye) reversal(1, pairing, players);
        }
    }


  organizeTournament();
