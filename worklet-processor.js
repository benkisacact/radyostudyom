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
    this.maxQueueSize = 50;
    this.port.onmessage = (e) => {
      if (this.queue.length >= this.maxQueueSize) {
        this.queue.shift();
      }
      this.queue.push(e.data);
    };
  }
  process(inputs, outputs) {
    const output = outputs[0];
    if (!output || output.length === 0) return true;
    
    const channelData = output[0];
    const len = channelData.length;
    
    if (this.queue.length > 0) {
      const chunk = this.queue.shift();
      const copyLen = Math.min(chunk.length, len);
      channelData.set(chunk.subarray(0, copyLen));
      if (copyLen < len) {
        channelData.fill(0, copyLen);
      }
    } else {
      channelData.fill(0);
    }
    return true;
  }
}

registerProcessor('recorder-worklet', RecorderProcessor);
registerProcessor('player-worklet', PlayerProcessor);