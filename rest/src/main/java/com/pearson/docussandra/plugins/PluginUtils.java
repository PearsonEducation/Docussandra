package com.pearson.docussandra.plugins;

import com.pearson.docussandra.plugininterfaces.Plugin;
import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils class for working with plugins.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class PluginUtils {

  /**
   * Logger for this class.
   */
  private static final Logger logger = LoggerFactory.getLogger(PluginUtils.class);

  /**
   * Gets file references to any plugin jars in the users home directory. Will return any file in
   * the home directory that ends with ".jar" and starts with "plugin".
   *
   * @return An array of files that are deputy jars to be loaded.
   */
  public static File[] getPluginJars() {
    File homeDir = new File(System.getProperty("user.home"));
    logger
        .info("Searching home directory (" + homeDir.getAbsolutePath() + ") for plugin modules...");
    return homeDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File directory, String fileName) {
        return fileName.endsWith(".jar") && fileName.startsWith("plugin");
      }
    });

  }

  /**
   * For a specific jar file, this method will extract any implementing Plugin classes of the
   * specified type.
   *
   * @param pluginJar Plugin jar file from which to extract any concrete plugins.
   * @param pluginType Plugin types to extract.
   * @return An ArrayList of plugin classes.
   * @throws MalformedURLException If there is a problem reading the plugin jar.
   */
  public static ArrayList<Class<? extends Plugin>> getPluginsFromExternalJar(File pluginJar,
      Class pluginType) throws MalformedURLException {
    URL[] urlArray = new URL[1];
    urlArray[0] = pluginJar.toURI().toURL();
    URLClassLoader childJar = new URLClassLoader(urlArray);
    Reflections reflections =
        new Reflections(new ConfigurationBuilder().setUrls(urlArray).addClassLoader(childJar));// confusing,
                                                                                               // but
                                                                                               // it
                                                                                               // works
    ArrayList<Class<? extends Plugin>> csfi = new ArrayList(reflections.getSubTypesOf(pluginType));
    ArrayList<Class<? extends Plugin>> toReturn = new ArrayList<>();
    for (Class<? extends Plugin> c : csfi) {
      if (!Modifier.isAbstract(c.getModifiers()))// don't pick up abstract plugins, we can't
                                                 // implement them anyway
      {
        toReturn.add(c);
        logger.debug(
            "Read class: " + c.getCanonicalName() + " from jar: " + pluginJar.getAbsolutePath());
      }
    }
    return toReturn;
  }

}
