1. pip install git+https://github.com/Azure/decisionAI-plugin.git
2. Define your own plugin service, inherit from PluginService. Example: <https://github.com/Azure/decisionAI-plugin/blob/main/decisionai_plugin/sample/lr/lr_plugin_service.py>
3. Implement your own do_verify, do_train and do_inference if needed. 
   1. do_verify: verify parameters. Example: <https://github.com/Azure/decisionAI-plugin/blob/main/decisionai_plugin/sample/lr/lr_plugin_service.py>
   2. do_train
      1. Your plugin is not trainable, set "super().__init__(False)" in init. You don't need to implement do_train.
      2. Your plugin is trainable, you need to implement do_train in your plugin service.
         1. Read data from Kensho. The same with do_inference.
         2. Implement your own training logic and save model to model_dir.
   3. do_inference
      1. Read data from Kensho. Example: prepare_inference_data in <https://github.com/Azure/decisionAI-plugin/blob/main/decisionai_plugin/sample/lr/lr_plugin_service.py>
      2. Implement your own inference logic.
         1. Your plugin is not trainable, you need to:
            1. Implement the inference logic and save result back to Kensho(optional). Example: do_inference in <https://github.com/Azure/decisionAI-plugin/blob/main/decisionai_plugin/sample/lr/lr_plugin_service.py>
         2. Your plugin is trainable, now the model has been downloaded from AzureBlob and unpacked to model_dir, you need to:
            1. Load model from model_dir.
            2. Implement the inference logic and save result back to Kensho(optional). Example: do_inference in <https://github.com/Azure/decisionAI-plugin/blob/main/decisionai_plugin/sample/lr/lr_plugin_service.py>
4. Add config for your own plugin. Example: <https://github.com/Azure/decisionAI-plugin/blob/main/decisionai_plugin/sample/lr/config/service_config.yaml>
   1. All these config items are needed for framework. You should not change them by default.
   2. You can add your own config items if needed. You can access the items from self.config defined and initialized in PluginService.
5. Test/Debug your own plugin at local. Example: <https://github.com/Azure/decisionAI-plugin/blob/main/decisionai_plugin/sample/lr/test/lr_test.py>
6. Add requirement for your plugin. Example: <https://github.com/Azure/decisionAI-plugin/blob/main/decisionai_plugin/sample/lr/requirements.txt> 
7. Add entry point for your plugin. Example: <https://github.com/Azure/decisionAI-plugin/blob/main/decisionai_plugin/sample/lr/run_server.py>
8. Add docker file for your plugin. Example: <https://github.com/Azure/decisionAI-plugin/blob/main/decisionai_plugin/lr-plugin.Dockerfile>
9. Build docker image and deploy it to anywhere you like.
10. Register your own plugin to Kensho. Example: <https://aidice-app.azurewebsites.net/solution-application>
11. Add your own data feed in Kensho. Example: <https://aidice-app.azurewebsites.net/data-feed>
12. Add your own analysis group in Kensho. Example: <https://aidice-app.azurewebsites.net/time-series-group>
13. Creat your own plugin instance in analysis group you just created and enjoy. Example: <https://aidice-app.azurewebsites.net/time-series-group>
