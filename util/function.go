package util

func WaitSuccess(f func() error, errHook func(), doneHook func()) {
	for {
		if err := f(); err != nil {
			if errHook != nil {
				errHook()
			}
			continue
		}
		if doneHook != nil {
			doneHook()
		}
		break
	}
}
