// worklet-processor.js
class RecorderProcessor extends AudioWorkletProcessor {
  process(inputs) {
    const input = inputs[0];
    if (input.length > 0) {
      const channelData = input[0]; // Mono kanal verisi
      // Veriyi kopyalayarak ana thread'e gönder
      this.port.postMessage(channelData.slice(0));
    }
    return true;
  }
}

class PlayerProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = [];
    this.port.onmessage = (e) => {
      this.queue.push(e.data);
      // Kuyruk çok büyürse (ağ gecikmeleri vs.) eski verileri at
      if (this.queue.length > 50) this.queue.shift();
    };
  }
  process(inputs, outputs) {
    const output = outputs[0];
    if (output.length > 0) {
      const channelData = output[0];
      if (this.queue.length > 0) {
        const chunk = this.queue.shift();
        for (let i = 0; i < chunk.length; i++) {
          channelData[i] = chunk[i];
        }
      } else {
        // Kuyruk boşsa sessizlik üret
        for (let i = 0; i < channelData.length; i++) channelData[i] = 0;
      }
    }
    return true;
  }
}

registerProcessor('recorder-worklet', RecorderProcessor);
registerProcessor('player-worklet', PlayerProcessor);