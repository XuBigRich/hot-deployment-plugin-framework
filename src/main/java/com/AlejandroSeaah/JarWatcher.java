package com.AlejandroSeaah;


import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;


/**
 * Created by bjsheguihua on 2016/11/11.
 * 负责插件变动监听和插件加载，工作流程为启动时扫描插件目录并加载插件，然后启动JarFileListener类监听变动
 */
public class JarWatcher  extends  Thread {
    private  static Logger logger = LoggerFactory.getLogger(JarWatcher.class);
    private  int refreshInteral = 1 ; // in seconds , careful
    private  ServiceProvider sp  = null ;
    public JarWatcher(ServiceProvider sp ){
        this.sp = sp;
    }

    public void scanAndLoadPlugin(){
        logger.info("Scan the plugin folder : " + sp.jarFolderPath);
        try {
            //获取到plugin文件夹
            File file = new File(sp.jarFolderPath);
            //获取plugin文件夹下的所有jar包
            File[] tempList = file.listFiles();
            //遍历plugin下文件（遍历所有jar包）
            for (int i = 0; i < tempList.length; i++) {
                //确认文件夹下文件符合jar包特性且 在配置文件中 成功添加给jar2ConfigMapper
                if (tempList[i].isFile() && tempList[i].getName().toString().endsWith("jar") && (null != sp.getConfigByJar(tempList[i].getName().toString()))) {
                    logger.info("Detect file: " + tempList[i].getName().toString());
                    try {
                        String jarName = tempList[i].getName().toString();
                        //调用jarLoarder方法传入jarName
                        jarLoarder(jarName);
                    } catch (Exception e) {
                        logger.error(e.getMessage() );
                    }
                }
            }
        }catch (Exception e){
            logger.error(e.getMessage() );
        }
    }

    public void run (){
        logger.info("Start monitoring : " + sp.jarFolderPath);
        FileAlterationObserver observer = new FileAlterationObserver(sp.jarFolderPath,null, null); // 监控目录jarFolderPath
        observer.addListener(new JarFileListener(this));
        FileAlterationMonitor monitor = new FileAlterationMonitor(this.refreshInteral,observer);
        try {
            monitor.start();
        }catch (Exception e){
            logger.error(e.getMessage() );
        }
    }

    /**
     * 这个是初始化jar包的关键 将从jar包中选中一个类进行初始化
     * @param jarName 包名
     * @return 返回是否成功初始化jar包
     * @throws Exception
     */
    public Boolean jarLoarder(String jarName) throws Exception{

        logger.debug("jarLoarder() called, jarName : " + jarName);
        //如果不存存在这个jar包的配置信息
        if( null == sp.getConfigByJar(jarName)){
            //扫描配置文件 将所有配置信息 加载到配置对象中
            sp.seekMappingConfig(jarName);//seek to jar info in config
            //加载完后如果依然不存在这个jar包的配置文件 ，那么 就放弃这个jar包 返回false
            if( null == sp.getConfigByJar(jarName)) {
                return false;//not a valid jar name at least
            }
        }
//        jarUnloader(jarName);
        //初始化一个插件加载类
        PluginClassLoader loader = new PluginClassLoader();
        //获取到jar包全路径
        String pluginUrl =  "jar:file:" + sp.jarFolderPath+jarName+"!/";  //  be careful
        logger.info("plugin url: " +pluginUrl);

        URL url = null;
        try {
            //初始化一个url对象
            url = new URL(pluginUrl);
        } catch (MalformedURLException e) {
            logger.error(e.getMessage() );
            return false;
        }
        String classpath = null;
        try{
            //将url传入加载插件类对象
            loader.addURLFile(url);
            //判断jar2ConfigMapper 对象中 是否存在jar
            if( null != sp.getConfigByJar(jarName)) {
                //如果存在 执行装载 ，从配置文件中根据将classpath设置为 jar包的全路径，全路径下包含jar包的class文件
                classpath = sp.getConfigByJar(jarName).getClassPath().trim();
            }else{
                //如果不存在 执行卸载
                loader.unloadJarFile();
                logger.warn("Can NOT find class path in config, "+url.toString()+" file CAN NOT  be loaded!");
                return false;
            }
        }catch (Exception e){
            loader.unloadJarFile();
            e.printStackTrace();
            logger.error(e.getMessage());
            return false;
        }

        try {
        	logger.info("classpath is "+classpath);
            //传入一个URLClassLoader jar包的位置
            Class<?> forName = Class.forName(classpath, true, loader);
            //初始化这个类
            PluginInterface ins = (PluginInterface)forName.newInstance();
            MFConfig mfc =  sp.getConfigByJar(jarName);
            ins.init(mfc.getArgs());
            logger.info("instance is "+ins.getClass().getName());
            sp.topic2InstanceMapper.put(sp.getConfigByJar(jarName).getTopic(), ins);
            sp.classPath2InstanceMapper.put(sp.getConfigByJar(jarName).getClassPath(), ins);
            logger.info("plugin "+ jarName + " loaded successfully!");
        }catch (Exception e){
            e.printStackTrace();
            logger.error(e.getMessage());
            return false;
        }finally {
            loader.unloadJarFile();
        }
        return true;
    }

    public void jarUnloader(String jarName){
        logger.debug("jarUnloader() called, jarName : " + jarName);
        try {
            if (sp.classPath2InstanceMapper.containsKey( sp.getConfigByJar(jarName).getClassPath() )) {
                sp.classPath2InstanceMapper.remove(sp.getConfigByJar(jarName).getClassPath());
            }

            if (sp.topic2InstanceMapper.containsKey( sp.getConfigByJar(jarName).getTopic() )) {
                sp.topic2InstanceMapper.get(sp.getConfigByJar(jarName).getTopic()).close();
                sp.topic2InstanceMapper.remove(sp.getConfigByJar(jarName).getTopic());
            }

        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }
}
