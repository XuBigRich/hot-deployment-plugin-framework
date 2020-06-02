package com.AlejandroSeaah;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;


public class PluginClassLoader extends URLClassLoader {
    private  static Logger logger = LoggerFactory.getLogger(PluginClassLoader.class);


    private JarURLConnection cachedJarFile = null;
    public PluginClassLoader() {
        super(new URL[] {}, findParentClassLoader());
    }


    public void addURLFile(URL file) {
        logger.debug("addURLFile() called , URL: " + file.toString());
        try {
            //根据路径创建一个URL连接
            URLConnection uc = file.openConnection();
            //这个URL连接 属于JarURLConnection
            if (uc instanceof JarURLConnection) {
                uc.setUseCaches(true);
                ((JarURLConnection) uc).getManifest();
                cachedJarFile = (JarURLConnection)uc;
            }
        } catch (Exception e) {
            logger.error("Failed to cache plugin JAR file: " + file.toExternalForm());
            logger.error(e.getMessage() );
        }
        //将指定的URL附加到URL列表中以搜索类和资源。
        addURL(file);
    }


    public void unloadJarFile() {
        logger.debug("unloadJarFile() called ");
            try {
                logger.info("Unloading plugin JAR file " + cachedJarFile.getJarFile().getName());
                cachedJarFile.getJarFile().close();
                cachedJarFile = null;
            } catch (Exception e) {
                logger.error("Failed to unload JAR file: "+e.getMessage());
                logger.error(e.getMessage() );
            }
    }

    private static ClassLoader findParentClassLoader() {
        logger.debug("findParentClassLoader() called");
        ClassLoader parent = ServiceProvider.class.getClassLoader();
        if ( null  == parent ) {
            parent = PluginClassLoader.class.getClassLoader();
        }
        if (null  == parent ) {
            parent = ClassLoader.getSystemClassLoader();
        }
        return parent;
    }
}
