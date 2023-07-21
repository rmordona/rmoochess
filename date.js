function log(msg) {
 console.log(msg);
}

// TZ="America/Los_Angeles";
//process.env.TZ = 'America/Los_Angeles';

// TZ="UTC"
// process.env.TZ = "UTC";

const today = Date.now();

function daylocal(tdate) {
        return new Date(tdate).toLocaleString('en-us',
             { weekday:"short", year:"numeric", month:"short", day:"numeric",
             hour: "numeric", minute: "numeric"});
} 

const nDate1 = new Date().toLocaleString('en-US', {
  timeZone: 'America/Los_Angeles'
});

const UTCdate = function(dt) { return (new Date(new Date(dt).toUTCString().replace(/ GMT/,'')) / 1); };

y = UTCdate(today);

ddate = 1673882088000

var sss = daylocal(ddate) + ' GMT';
log(sss);

var xxx = daylocal(sss);
log(xxx);
