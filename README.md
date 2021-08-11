# syncfile
**Real-time file synchronization** 
## Features 

1. Monitor Multiple directories on client server.
2. Support set server base path, server path prefix for different client, server path for different client path, avoid same name file or directory.
3. Support compress data and send in real time.
4. Support multiple files and folder transferring with interruption resuming capability to protect transfer against network failure.

## restriction

1. Path can not include space.
2. no upload for file name with space and hidden file。
3. If file size decrease, will upload whole file again.

<img src="https://raw.githubusercontent.com/charlesgreat/syncfile/main/doc/syncfile.jpg" />
