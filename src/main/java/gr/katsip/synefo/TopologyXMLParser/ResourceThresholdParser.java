package gr.katsip.synefo.TopologyXMLParser;

import gr.katsip.synefo.storm.api.Pair;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.StringTokenizer;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

public class ResourceThresholdParser {

	private final String THRESHOLDS = "thresholds";

	private final String RESOURCE = "resource";

	private final String RESOURCE_ID = "key";

	private final String RESOURCE_VAL = "value";

	private HashMap<String, Pair<Number, Number>> thresholds;

	public ResourceThresholdParser() {
		thresholds = new HashMap<String, Pair<Number, Number> >();
	}

	public void parseThresholds(String fileName) {
		XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		try {
			InputStream in = new FileInputStream(fileName);
			XMLEventReader eventReader = inputFactory.createXMLEventReader(in);
			String resource_id = null;
			String resource_val = null;
			while(eventReader.hasNext()) {
				XMLEvent event = eventReader.nextEvent();
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart() == (THRESHOLDS)) {
					}
				}
				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart() == (RESOURCE)) {
						resource_id = null;
						resource_val = null;
					}
				}

				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(RESOURCE_ID)) {
						event = eventReader.nextEvent();
						resource_id = event.asCharacters().getData();
					}
				}

				if(event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					if(startElement.getName().getLocalPart().equals(RESOURCE_VAL)) {
						event = eventReader.nextEvent();
						resource_val = event.asCharacters().getData();
						if(resource_id != null && resource_val != null) {
							StringTokenizer strTok = new StringTokenizer(resource_id, "-");
							String resourceName = strTok.nextToken();
							String resourceBound = strTok.nextToken();
							if(thresholds.containsKey(resourceName)) {
								Pair<Number, Number> values = thresholds.get(resourceName);
								if(resourceBound.equals("min")) {
									if(resourceName.equals("cpu") || resourceName.equals("memory")) {
										values.lowerBound = new Double(Double.parseDouble(resourceBound));
									}else {
										values.lowerBound = new Integer(Integer.parseInt(resourceBound));
									}
								}else {
									if(resourceName.equals("cpu") || resourceName.equals("memory")) {
										values.upperBound = new Double(Double.parseDouble(resourceBound));
									}else {
										values.upperBound = new Integer(Integer.parseInt(resourceBound));
									}
								}
								thresholds.put(resourceName, values);
							}else {
								Pair<Number, Number> values = new Pair<Number, Number>();
								if(resourceBound.equals("min")) {
									if(resourceName.equals("cpu") || resourceName.equals("memory")) {
										values.lowerBound = new Double(Double.parseDouble(resourceBound));
									}else {
										values.lowerBound = new Integer(Integer.parseInt(resourceBound));
									}
								}else {
									if(resourceName.equals("cpu") || resourceName.equals("memory")) {
										values.upperBound = new Double(Double.parseDouble(resourceBound));
									}else {
										values.upperBound = new Integer(Integer.parseInt(resourceBound));
									}
								}
								thresholds.put(resourceName, values);
							}
						}
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public HashMap<String, Pair<Number, Number>> get_thresholds() {
		return thresholds;
	}
}
