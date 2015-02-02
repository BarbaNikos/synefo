package gr.katsip.synefo.TopologyXMLParser;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

public class TopologyParser {
	
	private final String SYNEFO_SERVER = "synefo-server";
	
	private final String SYNEFO_HOST = "host";
	
	private final String SYNEFO_PORT = "port";
	
	private final String COMPONENT = "component";
	
	private final String EXECUTORS = "executors";
	
	private final String STAT_REPORT_TIMESTAMP = "stat-report-timestamp";
	
	private final String SCALEOUT_EVENT = "scale-out-event";
	
	private final String ACTION = "action";

	private final String COMPONENT_A = "component-a";
	
	private final String COMPONENT_B = "component-b";
	
	private final String WAIT_TIME = "wait-time";
	
	private final String COMPONENT_TYPE = "type";
	
	private final String COMPONENT_NAME = "name";
	
	private final String DOWNSTREAM = "downstream";
	
	private final String TASK = "task";
	
	private String synEFOhost;
	
	private String synEFOport;
	
	private List<Component> components;
	
//	private List<ScaleOutEvent> scaleOutEvents;

	public List<Component> getComponents() {
		return components;
	}

	public void setComponents(List<Component> components) {
		this.components = components;
	}

//	public List<ScaleOutEvent> getScaleOutEvents() {
//		return scaleOutEvents;
//	}

//	public void setScaleOutEvents(List<ScaleOutEvent> scaleOutEvents) {
//		this.scaleOutEvents = scaleOutEvents;
//	}

	public String getSynEFOhost() {
		return synEFOhost;
	}

	public void setSynEFOhost(String synEFOhost) {
		this.synEFOhost = synEFOhost;
	}

	public String getSynEFOport() {
		return synEFOport;
	}

	public void setSynEFOport(String synEFOport) {
		this.synEFOport = synEFOport;
	}

	public void parseTopology(String fileName) {
		components = new ArrayList<Component>();
//		scaleOutEvents = new ArrayList<ScaleOutEvent>();
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        try {
			InputStream in = new FileInputStream(fileName);
			XMLEventReader eventReader = inputFactory.createXMLEventReader(in);
			
			Component component = null;
//			ScaleOutEvent scaleOutEvent = null;
			List<String> upstream_tasks = null;
			while(eventReader.hasNext()) {
				XMLEvent event = eventReader.nextEvent();
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart() == (COMPONENT)) {
						component = new Component();
						upstream_tasks = new ArrayList<String>();
					}else if(startElement.getName().getLocalPart() == (SCALEOUT_EVENT)) {
//						scaleOutEvent = new ScaleOutEvent();
					}else if(startElement.getName().getLocalPart() == (SYNEFO_SERVER)) {
						
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(SYNEFO_HOST)) {
						event = eventReader.nextEvent();
						synEFOhost = event.asCharacters().getData();
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(SYNEFO_PORT)) {
						event = eventReader.nextEvent();
						synEFOport = event.asCharacters().getData();
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(COMPONENT_TYPE)) {
						event = eventReader.nextEvent();
						component.setType(event.asCharacters().getData());
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(EXECUTORS)) {
						event = eventReader.nextEvent();
						component.setExecutors(Integer.parseInt(event.asCharacters().getData()));
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(STAT_REPORT_TIMESTAMP)) {
						event = eventReader.nextEvent();
						component.setStat_report_timestamp(Integer.parseInt(event.asCharacters().getData()));
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(COMPONENT_NAME)) {
						event = eventReader.nextEvent();
						component.setName(event.asCharacters().getData());
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(COMPONENT_A)) {
						event = eventReader.nextEvent();
//						scaleOutEvent.setComponentA(event.asCharacters().getData());
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(COMPONENT_B)) {
						event = eventReader.nextEvent();
//						scaleOutEvent.setComponentB(event.asCharacters().getData());
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(ACTION)) {
						event = eventReader.nextEvent();
//						scaleOutEvent.setAction(event.asCharacters().getData());
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(WAIT_TIME)) {
						event = eventReader.nextEvent();
//						scaleOutEvent.setWaitTime(Long.parseLong(event.asCharacters().getData()));
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(DOWNSTREAM)) {
						event = eventReader.nextEvent();
						upstream_tasks = new ArrayList<String>();
					}
				}
				
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(TASK)) {
						event = eventReader.nextEvent();
						upstream_tasks.add(event.asCharacters().getData());
					}
				}
				
				if(event.isEndElement()) {
					EndElement endElement = event.asEndElement();
					if(endElement.getName().getLocalPart() == (COMPONENT)) {
						component.setUpstreamTasks(upstream_tasks);
						upstream_tasks = null;
						components.add(component);
					}else if(endElement.getName().getLocalPart() == (SCALEOUT_EVENT)) {
//						scaleOutEvents.add(scaleOutEvent);
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
}
