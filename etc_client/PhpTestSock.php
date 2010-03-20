<?php
	// PHP�e�X�g�X�N���v�g
	// PHP�ł̃f�[�^�o�^
	// ������
	// 1.���s���[�h:"1" or "1.1" ro "2" or "2.1" or "2.3" or "3" or "4" or "8"
	// (1=�o�^(�����C���N�������g), 1.1=�o�^(�l�w��), 2=�f�[�^�擾(�����C���N�������g), 2.1=�擾(�l�w��), 2.3=�擾(JavaScript���s), 3=Tag�Œl��Key��Value��o�^(�����C���N�������g), 4=Tag�Œl��Key���擾(Tag�l�w��), 8=Key�l���w��ō폜))
	// 2.�}�X�^�[�m�[�hIP:127.0.0.1
	// 3.�}�X�^�[�m�[�hPort:8888
	// 4.���s��:0�`n(���s���[�h1.1�y��2.1�y��2.3�y��3�y��8���͓o�^�A�擾�A�폜������Key or Tag�l)
	// 5.�o�^�f�[�^:(���s���[�h1.1�y��2.3���̂ݗL�� 1.1���͓o�^������Value�l�A2.3���͎��s������JavaScript)

    require_once("OkuyamaClient.class.php");

	if ($argc > 3) {

		// �N���C�A���g�쐬
		$client = new OkuyamaClient();

		// �ڑ�(MasterServer�Œ�)
		if(!$client->connect($argv[2], $argv[3])) {
			print_r("Sever Connection refused !!");
			exit;
		}

/*
		// AutoConnect���[�h�Őڑ�
		$serverInfos = array();
		$serverInfos[0] = "localhost:8888";
		$serverInfos[1] = "localhost:8889";
		// ����MasterServer�̏���ݒ�
		$client->setConnectionInfos($serverInfos);

		// �����ڑ�
		if(!$client->autoConnect()) {
			// �S�Ă̌��̃T�[�o�ɂȂ���Ȃ�
			print_r("Sever Connection refused !!");
			exit;
		}
*/
		// ����
		if ($argv[1] === "1") {

			// �f�[�^�������̉񐔕��o�^
			for ($i = 0; $i < $argv[4]; $i++) {
				
				if(!$client->setValue("datasavekey_" . $i, "savedatavaluestr_" . $i)) {
					print_r("Registration failure");
				}
			}
		} else if ($argv[1] === "1.1") {

			// �f�[�^�������̉񐔕��o�^
			if(!$client->setValue($argv[4], $argv[5])) {
				print_r("Regist Error");
			}


		} else if ($argv[1] === "2") {

			// �f�[�^�������̉񐔕��擾
			for ($i = 0; $i < $argv[4]; $i++) {
				$ret = $client->getValue("datasavekey_" . $i);
				if ($ret[0] === "true") {
					print_r($ret[1]);
					print_r("\r\n");
				} else {
					print_r("There is no data");
					print_r("\r\n");
				}
			}
		} else if ($argv[1] === "2.1") {
			// �w���Key�l�Ńf�[�^���擾

			$ret = $client->getValue($argv[4]);
			if ($ret[0] === "true") {
				print_r($ret[1]);
				print_r("\r\n");
			} else if ($ret[0] === "false") {
				print_r("There is no data");
				print_r("\r\n");
			}
		} else if ($argv[1] === "2.3") {
			// �w���Key�l�Ńf�[�^���擾

			$ret = $client->getValueScript($argv[4], $argv[5]);
			if ($ret[0] === "true") {
				print_r($ret[1]);
				print_r("\r\n");
			} else if ($ret[0] === "false") {
				print_r("There is no data");
				print_r("\r\n");
			}
		} else if ($argv[1] === "3") {

			// �f�[�^�������̉񐔕��o�^(Tag��o�^)
			$counter = 0;
			for ($i = 0; $i < $argv[4]; $i++) {
				$tags = array();
				if ($counter === 0) {
					$tags[0] = "tag1";
					$counter++;
				} else if($counter === 1) {
					$tags[0] = "tag1";
					$tags[1] = "tag2";
					$counter++;
				} else if($counter === 2) {
					$tags[0] = "tag1";
					$tags[1] = "tag2";
					$tags[2] = "tag3";
					$counter++;
				} else if($counter === 3) {
					$tags[0] = "tag4";
					$counter++;
				} else if($counter === 4) {
					$tags[0] = "tag4";
					$tags[1] = "tag2";
					$counter = 0;
				}
				if(!$client->setValue("datasavekey_" . $i, "savedatavaluestr_" . $i, $tags)) {
					print_r("Registration failure");
				}
			}
		} else if ($argv[1] === "4") {

			// �f�[�^�������̉񐔕��擾(Tag�Ŏ擾)
			$counter = 0;
			var_dump($client->getTagKeys($argv[4]));

		} else if ($argv[1] === "7") {

			// �f�[�^�������̉񐔕��擾
			for ($i = 0; $i < $argv[4]; $i++) {
				$ret = $client->removeValue("datasavekey_" . $i);
				if ($ret[0] === "true") {
					// �폜����
					print_r($ret[1]);
					print_r("\r\n");
				} else if ($ret[0] === "false") {
					// Key�l�Ńf�[�^�Ȃ�
					print_r("There is no data");
					print_r("\r\n");
				} else if ($ret[0] === "error") {
					// �폜�����ŃG���[
					print_r($ret[1]);
					print_r("\r\n");
				}
			}
		} else if ($argv[1] === "8") {

			// ������Key�l�̃f�[�^���폜
			$ret = $client->removeValue($argv[4]);
			if ($ret[0] === "true") {
				// �폜����
				print_r($ret[1]);
				print_r("\r\n");
			} else if ($ret[0] === "false") {
				// Key�l�Ńf�[�^�Ȃ�
				print_r("There is no data");
				print_r("\r\n");
			} else if ($ret[0] === "error") {
				// �폜�����ŃG���[
				print_r($ret[1]);
				print_r("\r\n");
			}
		}


		$client->close();
	} else {
		print_r("Args Error!!");
	}
?>