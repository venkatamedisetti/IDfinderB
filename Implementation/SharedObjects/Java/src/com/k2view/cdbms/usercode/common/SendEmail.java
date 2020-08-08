package com.k2view.cdbms.usercode.common;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendEmail {
	static Logger log = LoggerFactory.getLogger(SendEmail.class.getName());
	
    public static void sendEmail(String to, String from, String host, int port, String username, String password, int ssl_port, String subject, String email_body, String attachment) throws Exception {
        if(to == null || to.trim().equals("")){
            throw new RuntimeException("fnSendEmail - To is mandatory field!, please check!");
        }

        if(from == null || from.trim().equals("")){
            throw new RuntimeException("fnSendEmail - From is mandatory field!, please check!");
        }

        if(host == null || host.trim().equals("")){
            throw new RuntimeException("fnSendEmail - Host is mandatory field!, please check!");
        }

        if(port == 0){
            throw new RuntimeException("fnSendEmail - Port is mandatory field!, please check!");
        }

        String HTML_TAG_PATTERN = "<(\"[^\"]*\"|'[^']*'|[^'\">])*>";
        Pattern pattern = Pattern.compile(HTML_TAG_PATTERN);

        Properties props = System.getProperties();
        props.put("mail.smtp.host", host);
        props.put("mail.smtp.port", port);

        if(ssl_port == 0){
            props.put("mail.smtp.starttls.enable", "true");
        }else{
            props.put("mail.smtp.socketFactory.port", ssl_port);
            props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        }

        Session session;
        if((username != null && !username.trim().equals("")) && (password != null && !password.trim().equals(""))){
            props.put("mail.smtp.auth", "true");
            session = Session.getInstance(props, new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password);
                }
            });
        }else{
			props.put("mail.smtp.auth", "false");
            session = Session.getInstance(props, null);
        }

        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(from));

        String[] toArr = to.split(",");
        InternetAddress[] sendTo = new InternetAddress[toArr.length];
        for (int i = 0; i <toArr.length; i++) {
            sendTo[i] = new InternetAddress(toArr[i]);
        }
        message.setRecipients(Message.RecipientType.TO, sendTo);
        message.setSubject(subject);

        Multipart multipart = new MimeMultipart();
        MimeBodyPart bodyPart = new MimeBodyPart();

        Matcher matcher = pattern.matcher(email_body);
        if(matcher.find()){
            bodyPart.setContent( email_body, "text/html; charset=utf-8" );
        }else{
            bodyPart.setText( email_body, "utf-8" );
        }

        multipart.addBodyPart(bodyPart);

        if(attachment != null){
            MimeBodyPart attPart = new MimeBodyPart();
            DataSource source = new FileDataSource(attachment);
            attPart.setDataHandler(new DataHandler(source));
            attPart.setFileName(attachment);
            multipart.addBodyPart(attPart);

        }

        message.setContent(multipart);

        Transport.send(message);


    }
}
