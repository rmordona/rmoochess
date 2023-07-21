let peerConnection;
const config = {
  iceServers: [
    {
      urls: ["stun:stun.l.google.com:19302"]
    }
  ]
};

const socket = io("http://localhost:3000", {
         transports: ["websocket", "polling"] // use WebSocket first, if available
});

const video = document.querySelector("video");

socket.on("offer", (id, description) => {
console.log("I got an offer from a broadcaster ...");
console.log(id);
  peerConnection = new RTCPeerConnection(config);
  peerConnection
    .setRemoteDescription(description)
    .then(() => peerConnection.createAnswer())
    .then(sdp => peerConnection.setLocalDescription(sdp))
    .then(() => {
console.log("thou shall answer ...");
      socket.emit("answer", id, peerConnection.localDescription);
    });
  peerConnection.ontrack = event => {
console.log("on track ....");
    video.srcObject = event.streams[0];
  };
  peerConnection.onicecandidate = event => {
    if (event.candidate) {
     console.log("emitting candidate ...");
      socket.emit("candidate", id, event.candidate);
    }
  };
});

socket.on("candidate", (id, candidate) => {
  peerConnection
    .addIceCandidate(new RTCIceCandidate(candidate))
    .catch(e => console.error(e));
});

socket.on("connect", () => {
  socket.emit("watcher");
});

socket.on("broadcaster", () => {
  socket.emit("watcher");
});

socket.on("disconnectPeer", id => {
  peerConnections[id].close();
  delete peerConnections[id];
});

window.onunload = window.onbeforeunload = () => {
  socket.close();
  peerConnection.close();
};



