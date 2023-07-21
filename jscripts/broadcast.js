const peerConnections = {};
const config = {
  iceServers: [
    {
      urls: ["stun:stun.l.google.com:19302"]
    }
  ]
};

// const socket = io.connect(window.location.origin);

const socket = io("http://localhost:3000", {
          transports: ["websocket", "polling"] // use WebSocket first, if available
});


const video = document.querySelector("video");

// Media contrains
const constraints = {
  video: { 
/*
      mandatory: {
          minWidth: 500, // Provide your own width, height and frame rate here
          minHeight: 300,
          minFrameRate: 30
      },
*/
      facingMode: "environment" 
  }
  // Uncomment to enable audio
  // audio: true,
};


navigator.mediaDevices
  .getUserMedia(constraints)
  .then(stream => {
    video.srcObject = stream;
    socket.emit("broadcaster");
  })
  .catch(error => console.error(error));

navigator.getUserMedia(function (stream) {
  pc.addStream(stream);
}, err => console.log('Error getting User Media'));


socket.on("watcher", id => {
console.log("we got a watcher ...");
  const peerConnection = new RTCPeerConnection(config);
  peerConnections[id] = peerConnection;

  let stream = video.srcObject;
  stream.getTracks().forEach(track => peerConnection.addTrack(track, stream));
    
  peerConnection.onicecandidate = event => {
    if (event.candidate) {
      socket.emit("candidate", id, event.candidate);
    }
  };

  peerConnection
    .createOffer()
    .then(sdp => peerConnection.setLocalDescription(sdp))
    .then(() => {
      socket.emit("offer", id, peerConnection.localDescription);
    });
});

socket.on("answer", (id, description) => {
console.log("I got answer from watcher ...");
  peerConnections[id].setRemoteDescription(description);
});

socket.on("candidate", (id, candidate) => {
console.log("receiving candidate from watcher ...");
  peerConnections[id].addIceCandidate(new RTCIceCandidate(candidate));
});


socket.on("disconnectPeer", id => {
  peerConnections[id].close();
  delete peerConnections[id];
});


window.onunload = window.onbeforeunload = () => {
  socket.close();
};
