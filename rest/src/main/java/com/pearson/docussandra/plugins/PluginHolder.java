package com.pearson.docussandra.plugins;

import com.pearson.docussandra.plugininterfaces.NotifierPlugin;
import com.pearson.docussandra.plugininterfaces.Plugin;
import com.pearson.docussandra.plugininterfaces.SecurityPlugin;
import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for holding our instantiated plugin classes.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class PluginHolder
{

    /**
     * Logger for this class.
     */
    private static final Logger logger = LoggerFactory.getLogger(PluginHolder.class);

    /**
     * Static instance of this Plugin holder.
     */
    private static PluginHolder instance;

    /**
     * Instantiated Notifier plugins.
     */
    private static ArrayList<NotifierPlugin> notifierPlugins = new ArrayList<>();

    /**
     * Instantiated Notifier plugins.
     */
    private static ArrayList<SecurityPlugin> securityPlugins = new ArrayList<>();

    /**
     * Private constructor (singleton).
     */
    private PluginHolder()
    {
        ;
    }

    /**
     * Reads in all the possible plugins from the passed in plugin jars and
     * builds this object. Warning, this is a very expensive operation so it
     * should (and can be) only be called once. Call getInstance() to get a
     * reference to this object after build();
     *
     * @param pluginJars All the plugin jars we wish to load for this instance
     * of Docussandra.
     *
     * @return A newly created PluginHolder populated with all our plugins.
     * @throws MalformedURLException If there is a problem reading in the
     * plugins.
     * @throws InstantiationException If there is a problem creating a new
     * instance of one of the plugin classes.
     * @throws IllegalAccessException If we don't have permissions to create a
     * new plugin class (probably a private constructor issue?).
     */
    public static PluginHolder build(File[] pluginJars) throws MalformedURLException, InstantiationException, IllegalAccessException
    {
        if (instance != null)
        {
            throw new IllegalStateException("The PluginHolder has already been built. Please call getInstance() Instead.");
        }
        instance = new PluginHolder();
        ArrayList<Class<? extends Plugin>> notifierClasses = new ArrayList<>();
        ArrayList<Class<? extends Plugin>> securityClasses = new ArrayList<>();
        for (File pluginJar : pluginJars)
        {
            notifierClasses.addAll(PluginUtils.getPluginsFromExternalJar(pluginJar, NotifierPlugin.class));
            securityClasses.addAll(PluginUtils.getPluginsFromExternalJar(pluginJar, SecurityPlugin.class));
        }
        for (Class<? extends Plugin> clazz : notifierClasses)
        {
            NotifierPlugin object = (NotifierPlugin) clazz.newInstance();
            logger.info("Loading Notifier Plugin: " + object.getPluginName() + " of class type: " + object.getClass().getCanonicalName());
            notifierPlugins.add(object);
        }
        for (Class<? extends Plugin> clazz : securityClasses)
        {
            SecurityPlugin object = (SecurityPlugin) clazz.newInstance();
            logger.info("Loading Security Plugin: " + object.getPluginName() + " of class type: " + object.getClass().getCanonicalName());
            securityPlugins.add(object);
        }
        return instance;
    }

    /**
     * Gets a reference to this object. Must call build() to build the object
     * before calling this method.
     *
     * @return A reference to this PluginHolder object.
     */
    public static PluginHolder getInstance()
    {
        if (instance == null)
        {
            throw new IllegalStateException("The PluginHolder has not yet been built. Please call build() before calling get.");
        }
        return instance;
    }

    /**
     * @return the notifierPlugins
     */
    public ArrayList<NotifierPlugin> getNotifierPlugins()
    {
        return notifierPlugins;
    }

    /**
     * @return the securityPlugins
     */
    public ArrayList<SecurityPlugin> getSecurityPlugins()
    {
        return securityPlugins;
    }

}
