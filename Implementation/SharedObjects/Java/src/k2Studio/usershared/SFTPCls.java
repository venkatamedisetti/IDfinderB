package k2Studio.usershared;
//Auther:Nir Kehat
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SFTPCls {
	
		protected static Logger log = LoggerFactory.getLogger(SFTPCls.class.getName());	
        private String SFTPHOST;
        private final int SFTPPORT = 22;
        private String SFTPUSER;
        private String USERPWS;
        private String privateKey;
	    private JSch jSch = new JSch();
        private Session session = null;
	
	public SFTPCls(String SFTPHOST, String SFTPUSER, String USERPWS, String privateKey){
			this.SFTPHOST = SFTPHOST;
	        this.SFTPUSER = SFTPUSER;
	        this.USERPWS = USERPWS;
	        this.privateKey = privateKey;
		}
	
	public boolean connect(){
		try{
	        this.jSch.addIdentity(privateKey, USERPWS);
	        this.session = jSch.getSession(SFTPUSER,SFTPHOST,SFTPPORT);
	        java.util.Properties config = new java.util.Properties();
	        config.put("StrictHostKeyChecking", "no");
	        this.session.setConfig(config);
	        this.session.connect();
		}catch(JSchException e){
			log.error(e.getMessage());
			return false;
		}
			return true;
	}
	
	public void disconnect(){
        if(session!= null) session.disconnect();
	}
	
    public Set<String> sendFiles(String pathToCopyTo, String filePath, String fileNameRegex, String archPath) {
			Channel channel = null;
			ChannelSftp channelSftp = null;
			Set<String> fileListPass = new HashSet<String>();
        try {
            channel = session.openChannel("sftp");
            channel.connect();
            channelSftp = (ChannelSftp)channel;
            channelSftp.cd(pathToCopyTo);
	
			for(String fileToSend : getAllMacthingFilesLocal(filePath, fileNameRegex)){
				File fileToTrans = new File(filePath + "/" + fileToSend);
	            channelSftp.put(new FileInputStream(fileToTrans), fileToTrans.getName());
				fileListPass.add(pathToCopyTo + "/" + fileToSend);
				if(archPath != null){
					try{
						org.apache.commons.io.FileUtils.copyFile(fileToTrans, new File(archPath + "/" + fileToSend));
						fileToTrans.delete();
					}catch(IOException e){
						log.error(e.getMessage());
					}
				}
			}
        } catch (JSchException | SftpException | FileNotFoundException e) {
        	log.error(e.getMessage());
			return null;
		}finally{
            if(channelSftp!=null){
                channelSftp.disconnect();
                channelSftp.exit();
            }
            if(channel!=null) channel.disconnect();
        }
		return fileListPass;
    }
	
    public Set<String> getFiles(String destPath, String filePath, String fileNameRegex, String archPath) {
			Channel channel = null;
			ChannelSftp channelSftp = null;
			Set<String> fileListPass = new HashSet<String>();
	    try {
	        channel = session.openChannel("sftp");
			channel.connect();
	        channelSftp = (ChannelSftp)channel;
			channelSftp.cd(filePath);
			
			for(String fileToGet : getAllMacthingFiles(filePath, fileNameRegex, channelSftp)){
				channelSftp.get(fileToGet, destPath + "/" + fileToGet); 
				fileListPass.add(destPath + "/" + fileToGet);
				if(archPath != null){
					channelSftp.rename(filePath + "/" + fileToGet, archPath + "/" + fileToGet);
				}
			}
		
	    } catch (JSchException | SftpException e) {
	    	log.error(e.getMessage());
			return null;
		}finally{
	        if(channelSftp!= null){
	            channelSftp.disconnect();
	            channelSftp.exit();
	        }
	        if(channel!= null) channel.disconnect();
	    }
		return fileListPass;
    }
	
	private Set<String> getAllMacthingFiles(String filePath, String fileNameRegex, ChannelSftp channelSftp){
		Set<String> fileList = new HashSet<String>();
		java.util.regex.Matcher matcher = null;
		java.util.regex.Pattern fileNamePattern = java.util.regex.Pattern.compile("(?i)" + fileNameRegex);
		java.util.Vector<String> files;
		
		try{
			channelSftp.cd(filePath);
			files = channelSftp.ls("*");
		} catch (SftpException e) {
	    	log.error(e.getMessage());
			return null;
		}
		
		for (int i = 0; i < files.size(); i++){
			Object obj = files.elementAt(i);
		    if (obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry){
				com.jcraft.jsch.ChannelSftp.LsEntry entry = (com.jcraft.jsch.ChannelSftp.LsEntry) obj;
				if (!entry.getAttrs().isDir()){
		 			matcher = fileNamePattern.matcher(entry.getFilename());
					if (matcher.find())fileList.add(entry.getFilename());
				}
			}
		}
		return fileList;
	}
	
	private Set<String> getAllMacthingFilesLocal (String filePath, String fileNameRegex ){
		Set<String> fileList = new HashSet<String>();
		java.util.regex.Matcher matcher = null;
		java.util.regex.Pattern fileNamePattern = java.util.regex.Pattern.compile("(?i)" + fileNameRegex);
		File folder = new File(filePath);
		File[] listOfFiles = folder.listFiles();

	    for (int i = 0; i < listOfFiles.length; i++) {
	      if (listOfFiles[i].isFile()) {
	        matcher = fileNamePattern.matcher(listOfFiles[i].getName());
			if (matcher.find())fileList.add(listOfFiles[i].getName());
	      }
	    }
		return fileList;
	}
}
