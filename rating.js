
WHITE = 97
BLACK = 98

function log(m) { console.log(m) }

function eloRating(rA, rB, k, winner = BLACK) {
   pA = rA / ( rA + rB);
   pB = rB / ( rA + rB);
  if (winner == WHITE) {
   r1 = rA + k * ( 1 - pA);
   r2 = rB + k * ( 0 - pB);
  } else {
   r1 = rA + k * ( 0 - pA);
   r2 = rB + k * ( 1 - pB);
  }
  return { rA: Math.round(r1), rB: Math.round(r2) }
}


p1 = eloRating(1100, 900, 30, BLACK);
p2 = eloRating(1500, 1500, 30, BLACK);

console.log(p1);
console.log(p2);


function eloRating2(rA, rB, k, winner = BLACK) {
   pA = 1/ (1 +  Math.pow(10, (rB - rA) / 400 ));
   pB = 1/ (1 +  Math.pow(10, (rA - rB) / 400 ));
  if (winner == WHITE) {
   r1 = rA + k * ( 1 - pA);
   r2 = rB + k * ( 0 - pB);
  } else {
   r1 = rA + k * ( 0 - pA);
   r2 = rB + k * ( 1 - pB);
  }
   return { rA: Math.round(r1), rB: Math.round(r2) };
}

p1 = eloRating2(1200, 1000, 30,  WHITE);
p2 = eloRating2(2400, 2000, 32, WHITE);

console.log(p1);
console.log(p2);

function glicko2Rating(rA, rB, dA, dB, vA, vB) {
  const tau = 0.5 /* between 0.3 and 1.2 */ // controlls extreme improbable outcomes
  const sfactor = 173.7178;
  const R_ = 1500; /* if player is unrated */
  const RD_ = 350; /* if player is unrated */
  const epsilon = 0.000001; // tolerance
  var sA = 0.06;
  dA = 200;

  const p = function(d) { return d / sfactor; };  // scaled deviation
  var phi = p(dA);  // scaled deviation

  const u = function(R) { return (R - R_)/sfactor; }; // rating u
log("----");
  const myu  = u(1500),  
          u_ = [u(1400), u(1550), u(1700)],
          p_ = [p(30), p(100), p(300)];
log(myu);
log(u_[0]);
log(u_[1]);
log(u_[2]);
  const g = function(p) { return 1/ Math.sqrt( 1 + 3 * Math.pow(p, 2) / Math.pow(Math.PI,2) ); };
log("=====");
log(g(p_[0]));
log(g(p_[1]));
log(g(p_[2]));
  const E = function(u, u_j, p_j) { return 1/( 1 + Math.exp(-g(p_j) * (u - u_j)));  };
log("++++++");
log(E(myu, u_[0], p_[0]));
log(E(myu, u_[1], p_[1]));
log(E(myu, u_[2], p_[2]));
log("****");
  const V = function(u, u_, p_ ) {
              var v = 0, m = u_.length;
              for (var j=0; j<m; j++) {
                   v += Math.pow( g(p_[j]), 2) * E( u, u_[j], p_[j]) * ( 1 - E(u, u_[j], p_[j] ) );
              }
              return 1/v;
            }
  var v = V(myu, u_, p_);
  log(v);

log("#####");
  const s_ = [1, 0, 0];
  const D = function(u, u_, p_, s_, v) {
              var d = 0, m = u_.length;
              for (var j=0; j<m; j++) {
                 d += g(p_[j]) * ( s_[j] - E(u, u_[j], p_[j]) );
              } 
              return v * d;
            }

   const delta = D(myu, u_, p_, s_, v);
log(delta);



log("------");
  const f = function(x, d, v, p, tau) {
            return    ( Math.exp(x) * ( Math.pow(d, 2) - Math.pow(p, 2) - v - Math.exp(x)))  /
                      ( 2 * Math.pow( Math.pow(p, 2) + v + Math.exp(x) , 2)) - ( x - a ) / Math.pow(tau, 2);
            }

  var k = 1
  var A = a = Math.log(Math.pow(sA, 2));
  var B = a - k * tau;
  var C = null;
  log("%%%%%");
  log(A);
  log(B);

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
 
log(fA);
log(fB);

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

log("results ...");
   const sA_ = Math.exp(A/2);
log(sA_);

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
log(nphi_);
log(phi_);
log(myu_);

log("****");
   const rA_ = 173.7178 * myu_ + 1500;
   const dA_ = 173.7178 * phi_;
log(rA_);
log(dA_);
}

p1 = glicko2Rating(1200, 1000, 350,227.74, 0.06, 0.05999,  WHITE);
//p2 = glicko2Rating(2400, 2000, 350,227.74, 0.06, 0.05999,  WHITE);
