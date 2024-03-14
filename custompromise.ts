const promiseWithTimeout = (timeoutMs: number, promise: () => Promise<any>) => {
    return Promise.race([
      promise(),
      new Promise((resolve, reject) => setTimeout(() => reject(), timeoutMs)),
    ]);
  }