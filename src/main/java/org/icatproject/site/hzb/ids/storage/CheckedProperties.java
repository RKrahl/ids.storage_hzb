package org.icatproject.site.hzb.ids.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;


public class CheckedProperties {

    private Properties properties;

    public CheckedProperties(Properties properties) {
	this.properties = properties;
    }

    public boolean has(String name) {
	return properties.getProperty(name) != null;
    }

    public String getString(String name) throws IOException {
	String s = properties.getProperty(name);
	if (s == null) {
	    throw new IOException(name + " is not defined");
	}
	return s;
    }

    public boolean getBoolean(String name) throws IOException {
	return Boolean.parseBoolean(getString(name));
    }

    public int getOctalNumber(String name) throws IOException {
	int num;
	try {
	    num = Integer.parseInt(getString(name), 8);
	} catch (NumberFormatException e) {
	    throw new IOException(name + ": " + e.getMessage());
	}
	return num;
    }

    public Path getDirectory(String name) throws IOException {
	Path dir = Paths.get(getString(name));
	if (!Files.isDirectory(dir)) {
	    throw new IOException(name + ": " + dir + " is not a directory");
	}
	return dir;
    }

}
