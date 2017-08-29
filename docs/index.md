siddhi-map-wso2event
======================================

The **siddhi-map-wso2event extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi
</a> that  can be used to convert WSO2 events to/from Siddhi events.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-wso2event">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-wso2event/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-wso2event/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-wso2event/api/4.0.2-SNAPSHOT">4.0.2-SNAPSHOT</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-wso2event/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.map.wso2event</groupId>
        <artifactId>siddhi-map-wso2event-parent</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-wso2event/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-wso2event/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-wso2event/api/4.0.2-SNAPSHOT/#wso2event-sink-mapper">wso2event</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink-mappers">Sink Mapper</a>)*<br><div style="padding-left: 1em;"><p>Event to WSO2 output mapper. Transports which publish WSO2 messages, can utilize this extension to convert the Siddhi event to WSO2 event objects. Users can send pre-defined WSO2 event format which adheres to the defined stream.<br>Following prefixes will be used to identify different attributes such as meta, correlation, payload and arbitrary. Prefixes available,1. meta_ - metaData,2. correlation_ - correlationData,3. arbitrary_ - value contained in the arbitraryMap mapped to the key defined after the prefixIf the above prefixes are not used, the attribute is taken as payload data.<br>To enable custom mapping, all the attributes must be given as key value pair in the annotation, where the wso2 event attribute name key mapped to the siddhi attribute name.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-wso2event/api/4.0.2-SNAPSHOT/#wso2event-source-mapper">wso2event</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source-mappers">Source Mapper</a>)*<br><div style="padding-left: 1em;"><p>WSO2 Event to Siddhi Event input mapper. Transports which accepts WSO2 messages can utilize this extension to convert the incoming WSO2 message to Siddhi event. Users can send a WSO2 messagewhich should contain the mapping stream attributes in the same order as the defined stream (Within a WSO2 Event attribute type). This conversion will happen without any configs.<br>Following prefixes will be used to identify different attributes such as meta, correlation, payload and arbitrary. Prefixes available,1. meta_ - metaData,2. correlation_ - correlationData,3. arbitrary_ - value contained in the arbitraryMap mapped to the key defined after the prefixIf the above prefixes are not used, the attribute is taken as payload data.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-wso2event/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-wso2event/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
