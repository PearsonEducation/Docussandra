package com.pearson.docussandra.plugins;

import com.pearson.docussandra.plugininterfaces.NotifierPluginInterface;
import com.pearson.docussandra.plugininterfaces.Plugin;
import com.pearson.docussandra.plugininterfaces.SecurityPluginInterface;
import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;

/**
 * Class for holding our instantiated plugin classes.
 *
 * @author jeffrey
 */
public class PluginHolder
{

    /**
     * Static instance of this Plugin holder.
     */
    private static PluginHolder instance;

    /**
     * Instantiated Notifier plugins.
     */
    private static ArrayList<NotifierPluginInterface> notifierPlugins = new ArrayList<>();

    /**
     * Instantiated Notifier plugins.
     */
    private static ArrayList<SecurityPluginInterface> securityPlugins = new ArrayList<>();

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
     * @param pluginJars All the plugin jars we wish to load for this instance of Docussandra.
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
            notifierClasses.addAll(PluginUtils.getPluginsFromExternalJar(pluginJar, NotifierPluginInterface.class));
            securityClasses.addAll(PluginUtils.getPluginsFromExternalJar(pluginJar, SecurityPluginInterface.class));
        }
        for (Class<? extends Plugin> clazz : notifierClasses)
        {
            notifierPlugins.add((NotifierPluginInterface) clazz.newInstance());
        }
        for (Class<? extends Plugin> clazz : securityClasses)
        {
            securityPlugins.add((SecurityPluginInterface) clazz.newInstance());
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
    public ArrayList<NotifierPluginInterface> getNotifierPlugins()
    {
        return notifierPlugins;
    }

    /**
     * @return the securityPlugins
     */
    public ArrayList<SecurityPluginInterface> getSecurityPlugins()
    {
        return securityPlugins;
    }

}
