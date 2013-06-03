package org.ariadne_eu.oai.server.filesystem.crosswalk;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.ariadne.util.OaiUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.oclc.oai.server.crosswalk.Crosswalk;
import org.oclc.oai.server.verb.CannotDisseminateFormatException;

public class FileSystem2ODS extends Crosswalk {

	protected static Namespace lomns = Namespace
			.getNamespace("http://www.opendiscoveryspace.eu/metadata/v1/ods");
	protected static Namespace xsi = Namespace.getNamespace("xsi",
			"http://www.w3.org/2001/XMLSchema-instance");

	public FileSystem2ODS(Properties properties) {
		super(
				"http://www.opendiscoveryspace.eu/metadata/v1/ods file:///C:/Users/vogias/Desktop/PROJECTS/ODS/iNFO/WP7/ODS_xsd_xml/lomODS.xsd");//

	}

	public String createMetadata(Object nativeItem)
			throws CannotDisseminateFormatException {
		// Cast the nativeItem to your object
		try {
			SAXBuilder builder = new SAXBuilder();

			org.jdom.Document jdom = builder
					.build(new File((String) nativeItem));

			Element rootElement = jdom.getRootElement();
			/*Namespace namespace = rootElement.getNamespace();

			List<Element> children = rootElement.getChildren();

			for (int i = 0; i < children.size(); i++) {
				Element child = children.get(i);
				child.setNamespace(namespace);
				
			}*/
			return OaiUtils
					.parseLom2XmlstringNoXmlHeader(rootElement);//jdom.getRootElement()
		} catch (JDOMException e) {
			throw new CannotDisseminateFormatException(e.getMessage());
		} catch (IOException e) {
			throw new CannotDisseminateFormatException(e.getMessage());
		}
	}

	public boolean isAvailableFor(Object arg0) {
		return true;
	}

}
