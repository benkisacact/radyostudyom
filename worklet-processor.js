class RecorderProcessor extends AudioWorkletProcessor {
  process(inputs, outputs) {
    const input = inputs[0];
    const output = outputs[0]; // Bazı tarayıcıların çökmemesi için output referansı alıyoruz
    if (input && input.length > 0) {
      const channelData = input[0];
      if (channelData) this.port.postMessage(channelData.slice(0));
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
      if (this.queue.length > 50) this.queue.shift();
    };
  }
  process(inputs, outputs) {
    const output = outputs[0];
    if (output && output.length > 0) {
      const channelData = output[0];
      if (this.queue.length > 0) {
        const chunk = this.queue.shift();
        for (let i = 0; i < chunk.length; i++) {
          channelData[i] = chunk[i];
        }
      } else {
        for (let i = 0; i < channelData.length; i++) channelData[i] = 0;
      }
    }
    return true;
  }
}

registerProcessor('recorder-worklet', RecorderProcessor);
registerProcessor('player-worklet', PlayerProcessor);