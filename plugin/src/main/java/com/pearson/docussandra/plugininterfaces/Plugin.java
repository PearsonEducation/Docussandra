package com.pearson.docussandra.plugininterfaces;

/**
 * Plugin interface. All plugins must implement this interface. User-provided
 * plugins should <b>NOT</b> implement this directly, but instead should
 * implement one of the other provided interfaces that in turn implements this
 * class.
 * 
 * All implementing classes should be thread safe.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public interface Plugin
{

    /**
     * Gets the name of the implementing plugin. Used primarily for logging.
     * Should ideally be unique.
     *
     * @return A single string that is a meaningful name for this plugin.
     */
    public String getPluginName();

}
