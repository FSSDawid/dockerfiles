[![Docker Repository on Quay](https://quay.io/repository/v1k0d3n/homebridge/status "Docker Repository on Quay")](https://quay.io/repository/v1k0d3n/homebridge)

# Homebridge
As you could imagine, this is a container to run your homebridge services. As an example, you may want to run them on your Synology Diskstation, as documented [on my website JinkIT.com](http://www.jinkit.com/docker-on-synology/). Currently it supports the following homebridge services:

- [eDomoticz](https://github.com/PatchworkBoy/homebridge-edomoticz)
- [IFTTT](https://github.com/ilcato/homebridge-ifttt)
- [Liftmaster](https://github.com/nfarina/homebridge-liftmaster)
- [Luxtron](https://github.com/cbrandlehner/homebridge-luxtronik2)
- [MQTT](https://github.com/cflurin/homebridge-mqtt)
- [Nest](https://github.com/KraigM/homebridge-nest)
- Ninjablock Cloud ([Temp](https://www.npmjs.com/package/homebridge-ninjablock-temperature), [Humidity](https://www.npmjs.com/package/homebridge-ninjablock-humidity), [Alarm](https://www.npmjs.com/package/homebridge-ninjablock-alarmstatedevice))
- [People](https://www.npmjs.com/package/homebridge-people) (monitor who's at home with their smartphones)
- [Philips Hue](https://github.com/thkl/homebridge-philipshue)
- [Wink](https://github.com/bryanbartow/homebridge-wink)

If you would like to see additional packages installed, follow me on [Twitter](https://twitter.com/v1k0d3n) and/or send me a message.

To run:

```
docker run -d --net=host --name=homebridge -v /path/to/your/homebridge/directory:/root/.homebridge -p 51826:51826 v1k0d3n/homebridge
```
