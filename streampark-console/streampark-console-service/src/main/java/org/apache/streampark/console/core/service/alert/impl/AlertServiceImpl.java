package org.apache.streampark.console.core.service.alert.impl;

import org.apache.streampark.common.util.Utils;
import org.apache.streampark.console.base.exception.AlertException;
import org.apache.streampark.console.base.util.HttpUtils;
import org.apache.streampark.console.base.util.SpringContextUtils;
import org.apache.streampark.console.core.bean.AlertConfigWithParams;
import org.apache.streampark.console.core.bean.AlertTemplate;
import org.apache.streampark.console.core.entity.AlertConfig;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.enums.AlertType;
import org.apache.streampark.console.core.enums.CheckPointStatus;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.service.alert.AlertConfigService;
import org.apache.streampark.console.core.service.alert.AlertNotifyService;
import org.apache.streampark.console.core.service.alert.AlertService;

import org.apache.streampark.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.streampark.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class AlertServiceImpl implements AlertService {
  @Value("${web.server.ip}")
  private String webServerIp;

  @Value("${web.server.port}")
  private String webServerPort;

  @Value("${web.restart.authorization}")
  private String webRestartAuthorization;

  @Autowired private AlertConfigService alertConfigService;

  private ObjectMapper objectMapper = new ObjectMapper();
  private HttpUtils httpUtils = HttpUtils.init();

  @Override
  public void alert(Application application, CheckPointStatus checkPointStatus) {
    AlertTemplate alertTemplate = AlertTemplate.of(application, checkPointStatus);
    alert(application, alertTemplate);
  }

  @Override
  public void alert(Application application, FlinkAppState appState) {
    AlertTemplate alertTemplate = AlertTemplate.of(application, appState);
    alert(application, alertTemplate);
  }

  @Override
  public String restart(String jobName) {
    httpUtils.setHeader("Accept", "*/*");
    httpUtils.setHeader("Authorization", webRestartAuthorization);
    String baseUrl =
        new StringBuilder("http://")
            .append(webServerIp)
            .append(":")
            .append(webServerPort)
            .toString();
    String listUrl =
        new StringBuilder(baseUrl)
            .append("/flink/app/list?jobName=")
            .append(jobName)
            .append("&teamId=1&pageNum=1&pageSize=1")
            .toString();
    try {
      Map<String, String> postResult = httpUtils.post(listUrl);
      JsonNode rootNode = objectMapper.readTree(postResult.get("result"));
      JsonNode recordsNode = rootNode.path("data").path("records");
      if (recordsNode != null && recordsNode.isArray() && recordsNode.size() > 0) {
        String appId = recordsNode.get(0).path("id").asText();
        if (StringUtils.isNotBlank(appId)) {
          String startUrl =
              new StringBuilder(baseUrl)
                  .append("/flink/app/start?id=")
                  .append(appId)
                  .append("&savePointed=true")
                  .toString();
          postResult = httpUtils.post(startUrl);
          JsonNode resultNode = objectMapper.readTree(postResult.get("result"));
          String returnResult =
              resultNode.path("message").asText() == ""
                  ? "restart success"
                  : resultNode.path("message").asText();
          return returnResult;
        }
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  private void alert(Application application, AlertTemplate alertTemplate) {
    Integer alertId = application.getAlertId();
    if (alertId == null) {
      return;
    }
    AlertConfig alertConfig = alertConfigService.getById(alertId);
    try {
      alert(AlertConfigWithParams.of(alertConfig), alertTemplate);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  @Override
  public boolean alert(AlertConfigWithParams params, AlertTemplate alertTemplate)
      throws AlertException {
    List<AlertType> alertTypes = AlertType.decode(params.getAlertType());
    if (CollectionUtils.isEmpty(alertTypes)) {
      return true;
    }
    // No use thread pool, ensure that the alarm can be sent successfully
    Tuple2<Boolean, AlertException> reduce =
        alertTypes.stream()
            .map(
                alertType -> {
                  try {
                    Class<? extends AlertNotifyService> notifyServiceClass =
                        getAlertServiceImpl(alertType);
                    Utils.notNull(notifyServiceClass);
                    boolean alertRes =
                        SpringContextUtils.getBean(notifyServiceClass)
                            .doAlert(params, alertTemplate);
                    return new Tuple2<Boolean, AlertException>(alertRes, null);
                  } catch (AlertException e) {
                    return new Tuple2<>(false, e);
                  }
                })
            .reduce(
                new Tuple2<>(true, null),
                (tp1, tp2) -> {
                  boolean alertResult = tp1.f0 & tp2.f0;
                  if (tp1.f1 == null && tp2.f1 == null) {
                    return new Tuple2<>(tp1.f0 & tp2.f0, null);
                  }
                  if (tp1.f1 != null && tp2.f1 != null) {
                    // merge multiple exception, and keep the details of the first exception
                    AlertException alertException =
                        new AlertException(
                            tp1.f1.getMessage() + "\n" + tp2.f1.getMessage(), tp1.f1);
                    return new Tuple2<>(alertResult, alertException);
                  }
                  return new Tuple2<>(alertResult, tp1.f1 == null ? tp2.f1 : tp1.f1);
                });
    if (reduce.f1 != null) {
      throw reduce.f1;
    }

    return reduce.f0;
  }

  private Class<? extends AlertNotifyService> getAlertServiceImpl(AlertType alertType) {
    switch (alertType) {
      case EMAIL:
        return EmailAlertNotifyServiceImpl.class;
      case DING_TALK:
        return DingTalkAlertNotifyServiceImpl.class;
      case WE_COM:
        return WeComAlertNotifyServiceImpl.class;
      case LARK:
        return LarkAlertNotifyServiceImpl.class;
      case HTTP_CALLBACK:
        return HttpCallbackAlertNotifyServiceImpl.class;
      default:
        return null;
    }
  }
}
